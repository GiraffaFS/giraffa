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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

public class INode {
  // HdfsFileStatus fields
  private long length;
  private boolean isdir;
  private short block_replication;
  private long blocksize;
  private long modification_time;
  private long access_time;
  private FsPermission permission;
  private String owner;
  private String group;
  private byte[] path;
  private byte[] symlink;

  private long dsQuota;
  private long nsQuota;

  // Giraffa INode fields
  private final RowKey key;
  private DirectoryTable dirTable;
  private List<LocatedBlock> blocks;
  private FileState fileState;

  public static final Log LOG = LogFactory.getLog(INode.class.getName());

  /**
   * Construct an INode from the RowKey and file attributes.
   */
  public INode(long length, boolean directory, short replication, long blockSize,
      long mtime, long atime, FsPermission perms, String owner, String group,
      byte[] path, byte[] symlink, RowKey key, long dsQuota, long nsQuota,
      FileState state, DirectoryTable dirTable, List<LocatedBlock> blocks)
  throws IOException {
    this.length = length;
    this.isdir = directory;
    this.block_replication = replication;
    this.blocksize = blockSize;
    this.modification_time = mtime;
    this.access_time = atime;
    this.permission = perms;
    this.owner = owner;
    this.group = group;
    this.path = path;
    this.symlink = symlink;
    this.key = key;
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
    if(isDir()) {
      this.dirTable = (dirTable == null ? new DirectoryTable() : dirTable);
    } else {
      this.fileState = (state == null ? FileState.UNDER_CONSTRUCTION : state);
      this.blocks = (blocks == null? new ArrayList<LocatedBlock>() : blocks);
    }
  }

  public HdfsFileStatus getFileStatus() {
    return new HdfsFileStatus(length, isdir, block_replication,
           blocksize, modification_time, access_time, permission,
           owner, group, symlink, path);
  }

  public RowKey getRowKey() {
    return key;
  }

  /**
   * Get the length of this file, in bytes.
   * @return the length of this file, in bytes.
   */
  final public long getLen() {
    return length;
  }

  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  final public long getBlockSize() {
    return blocksize;
  }

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  final public short getReplication() {
    return block_replication;
  }

  /**
   * Get the modification time of the file.
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  final public long getModificationTime() {
    return modification_time;
  }

  /**
   * Get the access time of the file.
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  final public long getAccessTime() {
    return access_time;
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

  public long getDsQuota() {
    return dsQuota;
  }

  public long getNsQuota() {
    return nsQuota;
  }

  public boolean isDir() {
    return isdir;
  }

  public byte[] getSymlink() {
    return symlink;
  }

  public DirectoryTable getDirTable() {
    return dirTable;
  }

  public List<LocatedBlock> getBlocks() {
    return blocks;
  }

  public FileState getFileState() {
    return fileState;
  }

  /**
   * Get the blocks member as a byte array.
   * 
   * @return Byte array representation of the member blocks.
   * @throws IOException 
   */
  public byte[] getBlocksBytes() throws IOException {
    if(isDir())
      return null;

    byte[] retVal = null;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    try {
      for(LocatedBlock loc : blocks) {
        loc.write(out);
      }
      retVal = baos.toByteArray();
    } finally {
      try {
        out.close();
      } catch (IOException ignored) {}
    }
    return retVal;
  }

  public void setPermission(FsPermission newPermission) {
    this.permission = newPermission;
  }

  public void setQuota(long namespaceQuota, long diskspaceQuota) {
    if (namespaceQuota != FSConstants.QUOTA_DONT_SET) {
      this.nsQuota = namespaceQuota;
    }
    if (diskspaceQuota != FSConstants.QUOTA_DONT_SET) {
      this.dsQuota = diskspaceQuota;
    }
  }

  public void setReplication(short replication) {
    this.block_replication = replication;
  }

  public void setState(FileState newFileState) {
    this.fileState = newFileState;
  }

  public void setTimes(long mtime, long atime) {
    this.modification_time = mtime;
    this.access_time = atime;
  }

  public void setOwner(String username, String groupname) {
    this.owner = username;
    this.group = groupname;
  }

  public void setDirectoryTable(DirectoryTable dt) throws IOException {
    if(!isDir()) return;
    this.dirTable = (dt == null ? new DirectoryTable() : dt);
  }

  public void setLastBlock(Block last) {
    for(LocatedBlock block : blocks) {
      if(block.getBlock().getBlockId() == last.getBlockId()) {
        block.getBlock().set(
            last.getBlockId(), last.getNumBytes(), last.getGenerationStamp());
        return;
      }
    }
  }

  public String toString() {
    return "\"" + getRowKey().getPath() + "\":" + permission;
  }
}
