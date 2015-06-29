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

import static org.apache.hadoop.util.Time.now;

/**
 * Leases provide exclusive access to files for write.
 */
public class FileLease implements Comparable<FileLease> {

  public final String holder;
  public final String path;
  public final long lastUpdate;

  /**
   * Constructor for rebuilding from FileLeaseProto.
   */
  public FileLease(String holder, String path, long lastUpdate) {
    this.holder = holder;
    this.path = path;
    this.lastUpdate = lastUpdate;
  }

  public String getHolder() {
    return holder;
  }

  public String getPath() {
    return path;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public boolean expiredHardLimit(long hardLimit) {
    return now() - lastUpdate > hardLimit;
  }

  @Override
  public String toString() {
    return "FileLease[holder=" + holder + ", path=" + path + ", lastUpdate=" +
        lastUpdate + "]";
  }

  @Override
  public boolean equals(Object o) {
    if(!(o instanceof FileLease))
      return false;
    FileLease other = (FileLease) o;
    if(!holder.equals(other.holder))
      return false;
    if(!path.equals(other.path))
      return false;
    if(lastUpdate != other.lastUpdate)
      return false;
    return true;
  }

  @Override
  public int compareTo(FileLease o) {
    FileLease l1 = this;
    long lu1 = l1.lastUpdate;
    long lu2 = o.lastUpdate;
    if (lu1 < lu2) {
      return -1;
    } else if (lu1 > lu2) {
      return 1;
    } else {
      return l1.holder.compareTo(o.holder);
    }
  }
}
