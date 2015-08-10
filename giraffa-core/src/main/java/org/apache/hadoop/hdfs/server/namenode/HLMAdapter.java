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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.giraffa.FileLease;

/**
 * HDFS LeaseManager adapter.
 * Used to implement giraffa.LeaseManager for now.
 */
public class HLMAdapter {
  /** hdfs.LeaseManager */
  private final LeaseManager hlm;

  public HLMAdapter() {
    hlm = new LeaseManager(null);
  }

  public FileLease addLease(FileLease fileLease) {
    String holder = fileLease.getHolder();
    String path = fileLease.getPath();
    LeaseManager.Lease lease = hlm.addLease(holder, path);
    return wrapLease(lease, path);
  }

  public boolean removeLease(FileLease fileLease) {
    String holder = fileLease.getHolder();
    String path = fileLease.getPath();
    hlm.removeLease(holder, path);
    return getLeases(holder) == null;
  }

  public boolean removeLease(FileLease fileLease, String path) {
    String holder = fileLease.getHolder();
    hlm.removeLease(holder, path);
    return getLeases(holder) == null;
  }

  private static FileLease wrapLease(LeaseManager.Lease lease, String src) {
    if(lease == null)
      return null;
    else
      return new FileLease(lease.getHolder(), src, lease.getLastUpdate());
  }

  private Collection<FileLease> wrapLease(LeaseManager.Lease lease) {
    if(lease == null)
      return null;
    else {
      Collection<FileLease> leases = new ArrayList<FileLease>();
      for(String path : lease.getPaths()) {
        leases.add(wrapLease(lease, path));
      }
      return leases;
    }
  }

  public boolean isLeaseSoftLimitExpired(String holder) {
    LeaseManager.Lease lease = hlm.getLease(holder);
    return lease.expiredSoftLimit();
  }

  public void renewLease(String clientName) {
    hlm.renewLease(clientName);
  }

  public Collection<FileLease> getLeases(String holder) {
    LeaseManager.Lease lease = hlm.getLease(holder);
    return wrapLease(lease);
  }

  public SortedSet<FileLease> getSortedLeases() {
    SortedSet<LeaseManager.Lease> sortedLeases = hlm.getSortedLeases();
    SortedSet<FileLease> sortedFileLeases = new TreeSet<FileLease>();
    for(LeaseManager.Lease lease : sortedLeases) {
      sortedFileLeases.addAll(wrapLease(lease));
    }
    return sortedFileLeases;
  }
}
