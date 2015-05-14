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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.BlockManagementAgent;
import org.apache.giraffa.hbase.NamespaceProcessor;
import org.apache.hadoop.hdfs.server.namenode.HLMAdapter;

/**
 * Giraffa LeaseManager maintains leases for open files belonging to this
 * namespace partition.
 * 
 * Implemented as HDFS.LeaseManager, which is accessed through HLMAdapter.
 */
public class LeaseManager {

  private static final Log LOG =
      LogFactory.getLog(LeaseManager.class.getName());

  private static final ConcurrentMap<Object, LeaseManager> leaseManagerMap =
      new ConcurrentHashMap<Object, LeaseManager>();

  /**
   * Lease manager is a shared state between {@link NamespaceProcessor} and
   * {@link BlockManagementAgent}.
   * Any of them can instantiate LeaseManager if it has not been created yet.
   * Once created its reference is stored in a shared environment.
   */
  public synchronized static LeaseManager getLeaseManager(Object key) {
    LeaseManager leaseManager = leaseManagerMap.get(key);
    if(leaseManager == null) {
      leaseManager = new LeaseManager();
      LOG.info("Creating new LeaseManager for " + key);
      LeaseManager prevLeaseManager =
          leaseManagerMap.putIfAbsent(key, leaseManager);
      if(prevLeaseManager != null) {
        leaseManager = prevLeaseManager;
      }
    } else {
      LOG.info("LeaseManager already exists in shared state for " + key);
    }
    return leaseManager;
  }

  private HLMAdapter hlmAdapter;

  public LeaseManager() {
    this.hlmAdapter = new HLMAdapter();
  }

  public synchronized boolean removeLease(FileLease lease) {
    return hlmAdapter.removeLease(lease);
  }

  public synchronized FileLease addLease(FileLease lease) {
    return hlmAdapter.addLease(lease);
  }

  public boolean isLeaseSoftLimitExpired(String holder) {
    return hlmAdapter.isLeaseSoftLimitExpired(holder);
  }

  public synchronized Collection<FileLease> renewLease(String clientName) {
    hlmAdapter.renewLease(clientName);
    return hlmAdapter.getLeases(clientName);
  }

  @VisibleForTesting
  public Collection<FileLease> getLeases(String holder) {
    return hlmAdapter.getLeases(holder);
  }
}
