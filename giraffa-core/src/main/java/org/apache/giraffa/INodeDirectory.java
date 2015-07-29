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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;

import java.io.IOException;

/**
 * Stores all metadata related to a directory in the namespace, including the
 * associated quotas.
 */
public class INodeDirectory extends INode {

  private long dsQuota;
  private long nsQuota;

  /** This field is not serialized and may be null */
  private transient Boolean isEmpty;

  public INodeDirectory(RowKey key,
                        long mtime,
                        long atime,
                        String owner,
                        String group,
                        FsPermission permission,
                        byte[] symlink,
                        RenameState renameState,
                        long dsQuota,
                        long nsQuota) {
    super(key, mtime, atime, owner, group, permission, symlink, renameState);
    setQuota(nsQuota, dsQuota);
  }

  final public long getDsQuota() {
    return dsQuota;
  }

  final public long getNsQuota() {
    return nsQuota;
  }

  final public void setQuota(long namespaceQuota, long diskspaceQuota) {
    if (namespaceQuota != HdfsConstants.QUOTA_DONT_SET) {
      this.nsQuota = namespaceQuota;
    }
    if (diskspaceQuota != HdfsConstants.QUOTA_DONT_SET) {
      this.dsQuota = diskspaceQuota;
    }
  }

  /**
   * Returns whether this INode is an empty directory, or null if unknown.
   */
  final public Boolean isEmpty() {
    return isEmpty;
  }

  final public void setEmpty(boolean isEmpty) {
    this.isEmpty = isEmpty;
  }

  @Override // INode
  public boolean isDir() {
    return true;
  }

  @Override // INode
  public HdfsFileStatus getFileStatus() {
    return new HdfsFileStatus(0, true, 0, 0, getModificationTime(),
        getAccessTime(), getPermission(), getOwner(), getGroup(), getSymlink(),
        getPathBytes(), getId(), 0);
  }

  @Override // INode
  public HdfsLocatedFileStatus getLocatedFileStatus() {
    return new HdfsLocatedFileStatus(0, true, 0, 0, getModificationTime(),
        getAccessTime(), getPermission(), getOwner(), getGroup(), getSymlink(),
        getPathBytes(), getId(), null, 0);
  }

  @Override // INode
  public INodeDirectory cloneWithNewRowKey(RowKey newKey) {
    return new INodeDirectory(newKey, getModificationTime(), getAccessTime(),
        getOwner(), getGroup(), getPermission(), getSymlink(), getRenameState(),
        dsQuota, nsQuota);
  }

  public static INodeDirectory valueOf(INode node) throws IOException {
    if (node.isDir()) {
      return (INodeDirectory) node;
    } else {
      throw new IOException("Path is not a directory: " + node.getPath());
    }
  }
}
