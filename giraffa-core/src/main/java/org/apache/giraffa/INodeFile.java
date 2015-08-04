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

import static org.apache.giraffa.GiraffaConstants.FileState.UNDER_CONSTRUCTION;

import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores all metadata related to a file in the namespace, including the list of
 * blocks and locations.
 */
public class INodeFile extends INode {

  private long length;
  private short replication;
  private long blocksize;
  private FileState fileState;
  private FileLease lease;
  private List<UnlocatedBlock> blocks;
  private List<DatanodeInfo[]> locations;

  public INodeFile(RowKey key,
                   long mtime,
                   long atime,
                   String owner,
                   String group,
                   FsPermission permission,
                   byte[] symlink,
                   RenameState renameState,
                   long length,
                   short replication,
                   long blocksize,
                   FileState fileState,
                   FileLease lease,
                   List<UnlocatedBlock> blocks,
                   List<DatanodeInfo[]> locations) {
    super(key, mtime, atime, owner, group, permission, symlink, renameState);
    this.length = length;
    this.replication = replication;
    this.blocksize = blocksize;
    setState(fileState);
    this.lease = lease;
    setBlocks(blocks);
    setLocations(locations);
  }

  /**
   * Get the length of this file, in bytes.
   * @return the length of this file, in bytes.
   */
  final public long getLen() {
    return length;
  }

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  final public short getReplication() {
    return replication;
  }

  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  final public long getBlockSize() {
    return blocksize;
  }

  final public FileState getFileState() {
    return fileState;
  }

  final public FileLease getLease() {
    return lease;
  }

  final public List<UnlocatedBlock> getBlocks() {
    return blocks;
  }

  final public List<DatanodeInfo[]> getLocations() {
    return locations;
  }

  final public void setReplication(short replication) {
    this.replication = replication;
  }

  final public void setBlockSize(long blocksize) {
    this.blocksize = blocksize;
  }

  final public void setState(FileState fileState) {
    if (fileState == null) {
      fileState = UNDER_CONSTRUCTION;
    }
    this.fileState = fileState;
  }

  final public void setLease(FileLease lease) {
    this.lease = lease;
  }

  final public void setBlocks(List<UnlocatedBlock> blocks) {
    if (blocks == null) {
      blocks = new ArrayList<>();
    }
    this.blocks = blocks;
  }

  final public void setLastBlock(ExtendedBlock last) {
    for(UnlocatedBlock block : blocks) {
      ExtendedBlock eb = block.getBlock();
      if(eb.getBlockId() == last.getBlockId()) {
        eb.setNumBytes(last.getNumBytes());
        eb.setGenerationStamp(last.getGenerationStamp());
        return;
      }
    }
  }

  final public void setLocations(List<DatanodeInfo[]> locations) {
    if (locations == null) {
      locations = new ArrayList<>();
    }
    this.locations = locations;
  }

  @Override // INode
  public boolean isDir() {
    return false;
  }

  @Override // INode
  public HdfsFileStatus getFileStatus() {
    return new HdfsFileStatus(length, false, replication, blocksize,
        getModificationTime(), getAccessTime(), getPermission(), getOwner(),
        getGroup(), getSymlink(), getPathBytes(), getId(), 0);
  }

  @Override // INode
  public HdfsLocatedFileStatus getLocatedFileStatus() {
    List<LocatedBlock> locatedBlocksList =
        UnlocatedBlock.toLocatedBlocks(blocks, locations);
    int blks = locatedBlocksList.size();
    LocatedBlock lastBlock = blks == 0 ? null : locatedBlocksList.get(blks-1);
    boolean isUnderConstruction = (fileState == FileState.UNDER_CONSTRUCTION);
    boolean isLastBlockComplete = (fileState == FileState.CLOSED);
    LocatedBlocks locatedBlocks = new LocatedBlocks(length, isUnderConstruction,
        locatedBlocksList, lastBlock, isLastBlockComplete);
    return new HdfsLocatedFileStatus(length, false, replication, blocksize,
        getModificationTime(), getAccessTime(), getPermission(), getOwner(),
        getGroup(), getSymlink(), getPathBytes(), getId(), locatedBlocks, 0);
  }

  @Override // INode
  public INodeFile cloneWithNewRowKey(RowKey newKey) {
    return new INodeFile(newKey, getModificationTime(), getAccessTime(),
        getOwner(), getGroup(), getPermission(), getSymlink(), getRenameState(),
        length, replication, blocksize, fileState, lease, blocks, locations);
  }

  public static INodeFile valueOf(INode node) throws IOException {
    if (!node.isDir()) {
      return (INodeFile) node;
    } else {
      throw new IOException("Path is a directory: " + node.getPath());
    }
  }
}
