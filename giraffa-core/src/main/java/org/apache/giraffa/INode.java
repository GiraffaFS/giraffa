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
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.server.namenode.INodeId;

/**
 * Stores all metadata related to a file or directory in the namespace,
 * including the {@link RowKey} of the associated row in the database. The
 * fields of an {@code INode} are associated with both files and directories.
 */
public abstract class INode {

  private final RowKey key;
  private final long id;
  private long mtime;
  private long atime;
  private String owner;
  private String group;
  private FsPermission permission;
  private byte[] symlink;
  private RenameState renameState;

  /**
   * Construct an INode from the RowKey and file attributes.
   */
  INode(RowKey key,
        long mtime,
        long atime,
        String owner,
        String group,
        FsPermission permission,
        byte[] symlink,
        RenameState renameState) {
    this.key = key;
    this.id = INodeId.GRANDFATHER_INODE_ID;
    this.mtime = mtime;
    this.atime = atime;
    this.owner = owner;
    this.group = group;
    this.permission = permission;
    setSymlink(symlink);
    setRenameState(renameState);
  }

  final public String getPath() {
    return key.getPath();
  }

  final byte[] getPathBytes() {
    return RowKeyBytes.toBytes(getPath());
  }

  final public RowKey getRowKey() {
    return key;
  }

  final public long getId() {
    return id;
  }

  /**
   * Get the modification time of the file.
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  final public long getModificationTime() {
    return mtime;
  }

  /**
   * Get the access time of the file.
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  final public long getAccessTime() {
    return atime;
  }

  /**
   * Get FsPermission associated with the file.
   * @return permssion
   */
  final public FsPermission getPermission() {
    return permission;
  }

  /**
   * Get the owner of the file.
   * @return owner of the file
   */
  final public String getOwner() {
    return owner;
  }

  /**
   * Get the group associated with the file.
   * @return group for the file. 
   */
  final public String getGroup() {
    return group;
  }

  final public byte[] getSymlink() {
    return symlink;
  }

  final public RenameState getRenameState() {
    return renameState;
  }

  final public void setTimes(long mtime, long atime) {
    this.mtime = mtime;
    this.atime = atime;
  }

  final public void setOwner(String username, String groupname) {
    if(username != null) {
      this.owner = username;
    }
    if(groupname != null) {
      this.group = groupname;
    }
  }

  final public void setPermission(FsPermission newPermission) {
    this.permission = newPermission;
  }

  final public void setSymlink(byte[] symlink) {
    if (symlink != null) {
      symlink = symlink.clone();
    }
    this.symlink = symlink;
  }

  final public void setRenameState(RenameState renameState) {
    if (renameState == null) {
      renameState = RenameState.FALSE();
    }
    this.renameState = renameState;
  }

  public abstract boolean isDir();

  public abstract HdfsFileStatus getFileStatus();

  public abstract HdfsLocatedFileStatus getLocatedFileStatus();

  public abstract INode cloneWithNewRowKey(RowKey newKey);

  @Override
  public String toString() {
    return "\"" + getPath() + "\":" + permission;
  }
}
