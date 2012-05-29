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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.giraffa.GiraffaConstants.FileState;

class INode extends HdfsFileStatus {
  private final RowKey key;
  private final long dsQouta;
  private final long nsQouta;
  private DirectoryTable dirTable;
  private ArrayList<LocatedBlock> blocks;
  private FileState fileState;

  private Date expirationTime;

  private static final Log LOG = LogFactory.getLog(INode.class.getName());

  /**
   * Use for creating a new INode (must be put separately).
   */
  INode(long length, boolean directory, short replication, long blockSize,
      long mtime, long atime, FsPermission perms, String owner, String group,
      byte[] path, byte[] symlink, RowKey key, long dsQouta, long nsQouta) {
    super(length, directory, replication, blockSize, mtime, atime, perms,
        owner, group, path, symlink);
    this.key = key;
    this.nsQouta = nsQouta;
    this.dsQouta = dsQouta;
    this.fileState = FileState.UNDER_CONSTRUCTION;
    if(directory)
      this.dirTable = new DirectoryTable();
    else
      this.blocks = new ArrayList<LocatedBlock>();
  }

  /**
   * Construct an INode from the RowKey and the Result which was obtained
   * using the RowKey.
   * 
   * @param key
   * @param res
   */
  INode(RowKey key, Result res) {
    super(FileField.getLength(res),
        FileField.getDirectory(res),
        FileField.getReplication(res),
        FileField.getBlockSize(res),
        FileField.getMTime(res),
        FileField.getATime(res),
        FileField.getPermissions(res),
        FileField.getUserName(res),
        FileField.getGroupName(res),
        FileField.getSymlink(res),
        key.getPath().toString().getBytes());
    this.key = key;
    this.fileState = FileField.getState(res);
    this.nsQouta = FileField.getNsQuota(res);
    this.dsQouta = FileField.getDsQuota(res);
  }

  void putINode(HTable nsTable) throws IOException {
    Put put = new Put(key.getKey());
    put.add(FileField.getFileAttributes(), FileField.getFileName(),
            key.getPath().getName().getBytes())
        .add(FileField.getFileAttributes(), FileField.getSymlink(),
            key.getPath().toString().getBytes())
        .add(FileField.getFileAttributes(), FileField.getUserName(),
            getOwner().getBytes())
        .add(FileField.getFileAttributes(), FileField.getGroupName(),
            getGroup().getBytes())
        .add(FileField.getFileAttributes(), FileField.getLength(),
            Bytes.toBytes(getLen()))
        .add(FileField.getFileAttributes(), FileField.getPermissions(),
            Bytes.toBytes(getPermission().toShort()))
        .add(FileField.getFileAttributes(), FileField.getMTime(),
            Bytes.toBytes(getModificationTime()))
        .add(FileField.getFileAttributes(), FileField.getATime(),
            Bytes.toBytes(getAccessTime()))
        .add(FileField.getFileAttributes(), FileField.getDsQuota(),
            Bytes.toBytes(dsQouta))
        .add(FileField.getFileAttributes(), FileField.getNsQuota(),
            Bytes.toBytes(nsQouta))
        .add(FileField.getFileAttributes(), FileField.getReplication(),
            Bytes.toBytes(getReplication()))
        .add(FileField.getFileAttributes(), FileField.getBlockSize(),
            Bytes.toBytes(getBlockSize()))
        .add(FileField.getFileAttributes(), FileField.getState(),
            Bytes.toBytes(fileState.toString()));

    if(isDir())
      put.add(FileField.getFileAttributes(), FileField.getDirectory(), getDirectoryTableBytes());
    else {
      put.add(FileField.getFileAttributes(), FileField.getBlock(), getBlocksBytes());
    }

    nsTable.put(put);
  }

  /**
   * Should never be called without calling getBlocks() first.
   * This will get the blocks member as a byte array.
   * @return Byte array representation of the member blocks.
   * @throws IOException
   */
  private byte[] getBlocksBytes() {
    if(isDir())
      return null;

    byte[] retVal = null;

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      for(LocatedBlock loc : blocks) {
        loc.write(out);
      }
      retVal = baos.toByteArray();
    } catch (IOException e) {
      return null;
    }
    return retVal;
  }

  Date getExpirationTime() {
    return expirationTime;
  }

  void setExpirationTime(Date expirationTime) {
    this.expirationTime = expirationTime;
  }

  RowKey getRowKey() {
    return key;
  }

  long getDs_qouta() {
    return dsQouta;
  }

  long getNs_qouta() {
    return nsQouta;
  }

  /**
   * Get DirectoryTable from HBase.
   * 
   * @param nodeInfo
   * @return The directory table.
   */
  DirectoryTable getDirectoryTable(Result nodeInfo) {
    if(!isDir())
      return null;

    try {
      dirTable = new DirectoryTable(nodeInfo.getValue(
          FileField.getFileAttributes(), FileField.getDirectory()));
    } catch (IOException e) {
      LOG.info("Cannot get directory table", e);
      return null;
    } catch (ClassNotFoundException e) {
      LOG.info("Cannot get directory table", e);
      return null;
    }
    return dirTable;
  }

  /**
   * Get LocatedBlock info from HBase based on this
   * nodes internal RowKey.
   * @param nodeInfo
   * @return LocatedBlock from HBase row. Null if a directory or
   *  any sort of Exception happens.
   * @throws IOException
   */
  ArrayList<LocatedBlock> getBlocks(Result nodeInfo) throws IOException {
    if(isDir())
      return null;

    blocks = new ArrayList<LocatedBlock>();
    DataInputStream in = null;
    byte[] value = nodeInfo.getValue(
        FileField.getFileAttributes(), FileField.getBlock());
    in = new DataInputStream(new ByteArrayInputStream(value));
    while(in.available() > 0) {
      LocatedBlock loc = LocatedBlock.read(in);
      blocks.add(loc);
    }
    in.close();
    return blocks;
  }

  /**
   * Should never be called without calling getDirectoryTable() first. This will get the
   * dirTable member as a byte array.
   * @return Byte array representation of the member dirTable.
   * @throws IOException
   */
  private byte[] getDirectoryTableBytes() throws IOException {
    return dirTable.toBytes();
  }

  void updateDirectory(HTable nsTable) throws IOException {
    if(!isDir())
      return;

    Put put = new Put(key.getKey());
    put.add(FileField.getFileAttributes(), FileField.getDirectory(), getDirectoryTableBytes());
    nsTable.put(put);
  }

  void updatePermissions(HTable nsTable, FsPermission newPermission)
  throws IOException {
    Put put = new Put(key.getKey());
    put.add(FileField.getFileAttributes(), FileField.getPermissions(),
        Bytes.toBytes(newPermission.toShort()));
    nsTable.put(put);
  }

  void updateQuotas(HTable nsTable, long namespaceQuota, long diskspaceQuota)
  throws IOException {
    Put put = new Put(key.getKey());
    put.add(FileField.getFileAttributes(), FileField.getNsQuota(), Bytes.toBytes(namespaceQuota))
    .add(FileField.getFileAttributes(), FileField.getDsQuota(), Bytes.toBytes(diskspaceQuota));
    nsTable.put(put);
  }

  void updateReplication(HTable nsTable, short replication)
  throws IOException {
    Put put = new Put(key.getKey());
    put.add(FileField.getFileAttributes(), FileField.getReplication(), Bytes.toBytes(replication));
    nsTable.put(put);
  }

  void setState(FileState fileState) {
    this.fileState = fileState;
  }

  FileState getFileState() {
    return fileState;
  }

  void updateTimes(HTable nsTable, long mtime, long atime) throws IOException {
    Put put = new Put(key.getKey());
    put.add(FileField.getFileAttributes(), FileField.getMTime(), Bytes.toBytes(mtime))
    .add(FileField.getFileAttributes(), FileField.getATime(), Bytes.toBytes(atime));
    nsTable.put(put);
  }

  void updateOwner(HTable nsTable, String username, String groupname)
  throws IOException {
    Put put = new Put(key.getKey());
    put.add(FileField.getFileAttributes(), FileField.getUserName(), username.getBytes())
    .add(FileField.getFileAttributes(), FileField.getGroupName(), groupname.getBytes());
    nsTable.put(put);
  }

  void addBlock(HTable nsTable) throws IOException {
    Put put = new Put(key.getKey());
    put.add(FileField.getFileAttributes(), FileField.getBlock(), getBlocksBytes());
    nsTable.put(put);
  }

  void replaceBlock(Block last) {
    for(LocatedBlock block : blocks) {
      if(block.getBlock().getBlockId() == last.getBlockId()) {
        block.getBlock().set(last.getBlockId(), last.getNumBytes(), last.getGenerationStamp());
        return;
      }
    }
  }
}
