/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraffa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.BlockManagementAgent;
import org.apache.giraffa.hbase.NamespaceProcessor;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.HLMAdapter;
import org.apache.hadoop.util.Daemon;

/**
 * Giraffa LeaseManager maintains leases for open files belonging to this
 * namespace partition.
 * 
 * Implemented as HDFS.LeaseManager, which is accessed through HLMAdapter.
 */
public class LeaseManager {
  static final Log LOG = LogFactory.getLog(LeaseManager.class.getName());

  /**
   * The map is needed in unit tests with MiniCluster.
   * When multiple RegionServers run in the same JVM they should have
   * different instances of LeaseManager.
   */
  private static final ConcurrentMap<String, LeaseManager> leaseManagerMap =
      new ConcurrentHashMap<String, LeaseManager>();

  /**
   * Lease manager is a shared state between {@link NamespaceProcessor} and
   * {@link BlockManagementAgent}.
   * Any of them can instantiate LeaseManager if it has not been created yet.
   * Once created its reference is stored in a shared environment.
   */
  public synchronized static LeaseManager originateSharedLeaseManager(
      String key) {
    LeaseManager leaseManager = leaseManagerMap.get(key);
    if(leaseManager != null) {
      LOG.info("LeaseManager already exists in shared state for " + key);
      return leaseManager;
    }
    leaseManager = new LeaseManager();
    LOG.info("Creating new LeaseManager for " + key);
    LeaseManager prevLeaseManager =
        leaseManagerMap.putIfAbsent(key, leaseManager);
    if(prevLeaseManager != null) {
      leaseManager = prevLeaseManager;
    }
    return leaseManager;
  }

  private HLMAdapter hlmAdapter;
  private Daemon monitor;

  private volatile boolean shouldRunMonitor = false;
  private volatile long hardLimit = HdfsConstants.LEASE_HARDLIMIT_PERIOD;

  public LeaseManager() {
    this.hlmAdapter = new HLMAdapter();
  }

  public synchronized boolean removeLease(FileLease lease) {
    return hlmAdapter.removeLease(lease);
  }

  public synchronized FileLease addLease(FileLease lease) {
    return hlmAdapter.addLease(lease);
  }

  public synchronized void setHardLimit(long newHardLimit) {
    this.hardLimit = newHardLimit;
  }

  public boolean isLeaseSoftLimitExpired(String holder) {
    return hlmAdapter.isLeaseSoftLimitExpired(holder);
  }

  public synchronized Collection<FileLease> renewLease(String clientName) {
    hlmAdapter.renewLease(clientName);
    return hlmAdapter.getLeases(clientName);
  }

  public Collection<FileLease> getLeases(String holder) {
    return hlmAdapter.getLeases(holder);
  }

  public synchronized Daemon initializeMonitor(NamespaceProcessor namesystem) {
    if(monitor != null)
      return monitor;

    monitor = new Daemon(new LeaseMonitor(namesystem));
    return monitor;
  }

  public synchronized void triggerLeaseRecovery() {
    monitor.interrupt();
  }

  public void startMonitor() {
    assert monitor != null : "LeaseMonitor was not constructed.";
    if(shouldRunMonitor)
      return;
    shouldRunMonitor = true;
    monitor.start();
  }

  public void stopMonitor() {
    if (monitor != null) {
      shouldRunMonitor = false;
      try {
        monitor.interrupt();
        monitor.join(3000);
      } catch (InterruptedException ie) {
        LOG.warn("Encountered exception ", ie);
      }
      monitor = null;
    }
  }

  class LeaseMonitor implements Runnable {
    private final NamespaceProcessor namesystem;
    private final String name = getClass().getSimpleName();

    public LeaseMonitor(NamespaceProcessor namesystem) {
      this.namesystem = namesystem;
    }

    /** Check leases periodically. */
    @Override
    public void run() {
      while(shouldRunMonitor) {
        if(namesystem.isRunning())
          checkLeases();
        try {
          Thread.sleep(HdfsServerConstants.NAMENODE_LEASE_RECHECK_INTERVAL);
        } catch(InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted.", ie);
          }
        }
      }
      LOG.warn("Exiting LeaseMonitor.");
    }

    private void checkLeases() {
      LOG.debug("Checking leases.");

      SortedSet<FileLease> sortedLeases = hlmAdapter.getSortedLeases();
      if(sortedLeases.size() == 0)
        return;
      final FileLease oldest = sortedLeases.first();
      if (!oldest.expiredHardLimit(hardLimit)) {
        return;
      }

      LOG.info(oldest + " has expired hard limit.");

      final List<String> removing = new ArrayList<String>();
      // need to create a copy of the oldest lease paths, because
      // internalReleaseLease() removes paths corresponding to empty files,
      // i.e. it needs to modify the collection being iterated over
      // causing ConcurrentModificationException
      Collection<FileLease> leases = hlmAdapter.getLeases(oldest.getHolder());
      if(leases == null)
        return;
      for(FileLease lease : leases) {
        String p = lease.getPath();
        try {
          boolean completed = namesystem.internalReleaseLease(oldest, p);
          if (LOG.isDebugEnabled()) {
            if (completed) {
              LOG.debug("Lease recovery for " + p + " is complete." +
                  " File closed.");
            } else {
              LOG.debug("Started block recovery " + p + " lease " + oldest);
            }
          }
          if(!completed)
            return;
          // If a lease recovery happened, we need to sync later.
        } catch (IOException e) {
          LOG.error("Cannot release the path " + p + " in the lease "
              + oldest, e);
          removing.add(p);
        }
      }

      for(String p : removing) {
        hlmAdapter.removeLease(oldest, p);
      }
    }
  }
}
