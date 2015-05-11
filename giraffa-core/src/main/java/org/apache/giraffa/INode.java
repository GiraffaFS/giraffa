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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.INodeId;

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
  private byte[] symlink;
  private long fileId = INodeId.GRANDFATHER_INODE_ID;
  private int childrenNum = 0;

  private long dsQuota;
  private long nsQuota;

  // Giraffa INode fields
  private final RowKey key;
  private List<UnlocatedBlock> blocks;
  private List<DatanodeInfo[]> locations;
  private FileState fileState;
  private RenameState renameState;
  private FileLease lease;

  public static final Log LOG = LogFactory.getLog(INode.class.getName());

  /**
   * Construct an INode from the RowKey and file attributes.
   */
  public INode(long length, boolean directory, short replication, long blockSize,
      long mtime, long atime, FsPermission perms, String owner, String group,
      byte[] symlink, RowKey key, long dsQuota, long nsQuota, FileState state,
      RenameState renameState, List<UnlocatedBlock> blocks,
      List<DatanodeInfo[]> locations, FileLease lease) {
    this.length = length;
    this.isdir = directory;
    this.block_replication = replication;
    this.blocksize = blockSize;
    this.modification_time = mtime;
    this.access_time = atime;
    this.permission = perms;
    this.owner = owner;
    this.group = group;
    this.symlink = symlink == null ? null : symlink.clone();
    this.key = key;
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
    this.renameState = renameState == null ? RenameState.FALSE() : renameState;
    if(! isDir()) {
      this.fileState = (state == null ? FileState.UNDER_CONSTRUCTION : state);
      this.blocks = (blocks == null ? new ArrayList<UnlocatedBlock>() : blocks);
      this.locations = (locations == null ?
          new ArrayList<DatanodeInfo[]>() : locations);
      this.lease = (lease == null ? null : lease);
    }
  }

  public HdfsFileStatus getFileStatus() {
    return new HdfsFileStatus(length, isdir, block_replication, blocksize,
        modification_time, access_time, permission, owner, group, symlink,
        RowKeyBytes.toBytes(key.getPath()), fileId, childrenNum);
  }

  public HdfsFileStatus getLocatedFileStatus() {
    List<LocatedBlock> locatedBlocksList =
        UnlocatedBlock.toLocatedBlocks(blocks, locations);
    LocatedBlock lastBlock = locatedBlocksList.get(locatedBlocksList.size()-1);
    boolean isUnderConstruction = (fileState == FileState.UNDER_CONSTRUCTION);
    boolean isLastBlockComplete = (fileState == FileState.CLOSED);
    LocatedBlocks locatedBlocks = new LocatedBlocks(length, isUnderConstruction,
        locatedBlocksList, lastBlock, isLastBlockComplete);
    return new HdfsLocatedFileStatus(length, isdir, block_replication,
        blocksize, modification_time, access_time, permission, owner, group,
        symlink, RowKeyBytes.toBytes(key.getPath()), fileId, locatedBlocks,
        childrenNum);
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
    return symlink == null ? null : symlink.clone();
  }

  public List<UnlocatedBlock> getBlocks() {
    return blocks;
  }
  
  public List<DatanodeInfo[]> getLocations() {
    return locations;
  }

  public FileState getFileState() {
    return fileState;
  }

  public RenameState getRenameState() {
    return renameState;
  }

  public FileLease getLease() {
    return lease;
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
    else
      return GiraffaPBHelper.unlocatedBlocksToBytes(blocks);
  }
  
  public byte[] getLocationsBytes() throws IOException {
    if(isDir())
      return null;
    else
      return GiraffaPBHelper.blockLocationsToBytes(locations);
  }

  public byte[] getRenameStateBytes() {
    return renameState == null ? null :
      GiraffaPBHelper.convert(renameState).toByteArray();
  }

  public byte[] getLeaseBytes() throws IOException {
    if(isDir())
      return null;
    else
      return GiraffaPBHelper.hdfsLeaseToBytes(lease);
  }

  public void setPermission(FsPermission newPermission) {
    this.permission = newPermission;
  }

  public void setQuota(long namespaceQuota, long diskspaceQuota) {
    if (namespaceQuota != HdfsConstants.QUOTA_DONT_SET) {
      this.nsQuota = namespaceQuota;
    }
    if (diskspaceQuota != HdfsConstants.QUOTA_DONT_SET) {
      this.dsQuota = diskspaceQuota;
    }
  }

  public void setReplication(short replication) {
    this.block_replication = replication;
  }

  public void setState(FileState newFileState) {
    this.fileState = newFileState;
  }

  public void setRenameState(RenameState renameState) {
    this.renameState = renameState;
  }

  public void setTimes(long mtime, long atime) {
    this.modification_time = mtime;
    this.access_time = atime;
  }

  public void setOwner(String username, String groupname) {
    if(username != null) {
      this.owner = username;
    }
    if(groupname != null) {
      this.group = groupname;
    }
  }

  public void setLastBlock(ExtendedBlock last) {
    for(UnlocatedBlock block : blocks) {
      ExtendedBlock eb = block.getBlock();
      if(eb.getBlockId() == last.getBlockId()) {
        eb.setNumBytes(last.getNumBytes());
        eb.setGenerationStamp(last.getGenerationStamp());
        return;
      }
    }
  }

  public void setBlocks(List<UnlocatedBlock> blocks) {
    this.blocks = blocks;
  }

  public void setLocations(List<DatanodeInfo[]> locations) {
    this.locations = locations;
  }

  public void setLease(FileLease lease) {
    this.lease = lease;
  }

  public INode cloneWithNewRowKey(RowKey newKey) {
    return new INode(length, isdir, block_replication, blocksize,
        modification_time, access_time, permission, owner, group, symlink,
        newKey, dsQuota, nsQuota, fileState, renameState, blocks, locations,
        lease);
  }

  @Override
  public String toString() {
    return "\"" + getRowKey().getPath() + "\":" + permission;
  }
}
