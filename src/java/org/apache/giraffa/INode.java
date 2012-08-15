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
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.giraffa.GiraffaConstants.BlockAction;
import org.apache.giraffa.GiraffaConstants.FileState;

import static org.apache.hadoop.hdfs.server.common.Util.now;

public class INode {
  //HdfsFileStatus specific fields
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
  private long dsQouta;
  private long nsQouta;

  //INode specific fields
  private final RowKey key;
  private DirectoryTable dirTable;
  private ArrayList<LocatedBlock> blocks;
  private FileState fileState;
  private BlockAction blockAction;

  private Date expirationTime;

  private Put put;

  private static final Log LOG = LogFactory.getLog(INode.class.getName());

  /**
   * Use for creating a new INode (must be put separately).
   * @throws IOException 
   */
  public INode(long length, boolean directory, short replication, long blockSize,
      long mtime, long atime, FsPermission perms, String owner, String group,
      byte[] path, byte[] symlink, RowKey key, long dsQouta, long nsQouta) throws IOException {
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
    this.nsQouta = nsQouta;
    this.dsQouta = dsQouta;
    this.fileState = FileState.UNDER_CONSTRUCTION;
    if(isDir())
      this.dirTable = new DirectoryTable();
    else
      this.blocks = new ArrayList<LocatedBlock>();

    long ts = now();
    this.put = new Put(key.getKey(), ts);
    put.add(FileField.getFileAttributes(), FileField.getFileName(), ts,
            key.getPath().getName().getBytes())
        .add(FileField.getFileAttributes(), FileField.getSymlink(), ts,
            key.getPath().toString().getBytes())
        .add(FileField.getFileAttributes(), FileField.getUserName(), ts,
            owner.getBytes())
        .add(FileField.getFileAttributes(), FileField.getGroupName(), ts,
            group.getBytes())
        .add(FileField.getFileAttributes(), FileField.getLength(), ts,
            Bytes.toBytes(length))
        .add(FileField.getFileAttributes(), FileField.getPermissions(), ts,
            Bytes.toBytes(permission.toShort()))
        .add(FileField.getFileAttributes(), FileField.getMTime(), ts,
            Bytes.toBytes(modification_time))
        .add(FileField.getFileAttributes(), FileField.getATime(), ts,
            Bytes.toBytes(access_time))
        .add(FileField.getFileAttributes(), FileField.getDsQuota(), ts,
            Bytes.toBytes(dsQouta))
        .add(FileField.getFileAttributes(), FileField.getNsQuota(), ts,
            Bytes.toBytes(nsQouta))
        .add(FileField.getFileAttributes(), FileField.getReplication(), ts,
            Bytes.toBytes(block_replication))
        .add(FileField.getFileAttributes(), FileField.getBlockSize(), ts,
            Bytes.toBytes(blocksize))
        .add(FileField.getFileAttributes(), FileField.getState(), ts,
            Bytes.toBytes(fileState.toString()));

    if(isDir())
      put.add(FileField.getFileAttributes(), FileField.getDirectory(), ts,
             getDirectoryTableBytes());
    else
      put.add(FileField.getFileAttributes(), FileField.getBlock(), ts,
             getBlocksBytes());
  }

  public HdfsFileStatus getFileStatus() {
    return new HdfsFileStatus(length, isdir, block_replication,
           blocksize, modification_time, access_time, permission,
           owner, group, symlink, path);
  }

  /**
   * Construct an INode from the RowKey and the Result which was obtained
   * using the RowKey.
   * 
   * @param key
   * @param res
   * @throws IOException 
   */
  public INode(RowKey key, Result res) throws IOException {
    this.length = FileField.getLength(res);
    this.isdir = FileField.getDirectory(res);
    this.block_replication = FileField.getReplication(res);
    this.blocksize = FileField.getBlockSize(res);
    this.modification_time = FileField.getMTime(res);
    this.access_time = FileField.getATime(res);
    this.permission = FileField.getPermissions(res);
    this.owner = FileField.getUserName(res);
    this.group = FileField.getGroupName(res);
    this.path = key.getPath().toString().getBytes();
    this.symlink = FileField.getSymlink(res);
    this.key = key;
    this.fileState = FileField.getState(res);
    this.nsQouta = FileField.getNsQuota(res);
    this.dsQouta = FileField.getDsQuota(res);

    long ts = now();
    this.put = new Put(key.getKey(), ts);

    if(isDir()) {
      this.dirTable = getDirectoryTable(res);
    } else
      this.blocks = getBlocks(res);
  }

  public void updateINode(HTableInterface nsTable) throws IOException {
    if(put.size() == 0) {
      return;
    }
    nsTable.put(put);
  }

  /**
   * Should never be called without calling getBlocks() first.
   * This will get the blocks member as a byte array.
   * @return Byte array representation of the member blocks.
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

  public RowKey getRowKey() {
    return key;
  }

  long getDs_qouta() {
    return dsQouta;
  }

  long getNs_qouta() {
    return nsQouta;
  }

  public boolean isDir() {
    return isdir;
  }

  public long getBlockSize() {
    return blocksize;
  }

  /**
   * Get DirectoryTable from HBase.
   * 
   * @param nodeInfo
   * @return The directory table.
   */
  public DirectoryTable getDirectoryTable(Result nodeInfo) {
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
  public ArrayList<LocatedBlock> getBlocks(Result nodeInfo) throws IOException {
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

  public void setDirectoryTable() throws IOException {
    if(isDir()) {
      long ts = now();
      put.add(FileField.getFileAttributes(), FileField.getDirectory(), ts,
             getDirectoryTableBytes());
    }
  }

  public void setPermissions(FsPermission newPermission) {
    this.permission = newPermission;
    long ts = now();
    put.add(FileField.getFileAttributes(), FileField.getPermissions(), ts,
           Bytes.toBytes(permission.toShort()));
  }

  public void setQuotas(long namespaceQuota, long diskspaceQuota) {
    this.nsQouta = namespaceQuota;
    this.dsQouta = diskspaceQuota;
    long ts = now();
    put.add(FileField.getFileAttributes(), FileField.getNsQuota(), ts,
           Bytes.toBytes(nsQouta))
       .add(FileField.getFileAttributes(), FileField.getDsQuota(), ts,
            Bytes.toBytes(dsQouta));
  }

  public void setReplication(short replication) {
    this.block_replication = replication;
    long ts = now();
    put.add(FileField.getFileAttributes(), FileField.getReplication(), ts,
           Bytes.toBytes(block_replication));
  }

  public void setState(FileState newFileState) {
    this.fileState = newFileState;
    long ts = now();
    put.add(FileField.getFileAttributes(), FileField.getState(), ts,
           Bytes.toBytes(fileState.toString()));
  }

  public FileState getFileState() {
    return fileState;
  }

  public void setTimes(long mtime, long atime) {
    this.modification_time = mtime;
    this.access_time = atime;
    long ts = now();
    put.add(FileField.getFileAttributes(), FileField.getMTime(), ts,
           Bytes.toBytes(modification_time))
       .add(FileField.getFileAttributes(), FileField.getATime(), ts,
            Bytes.toBytes(access_time));
  }

  public void setOwner(String username, String groupname) {
    this.owner = username;
    this.group = groupname;
    long ts = now();
    put.add(FileField.getFileAttributes(), FileField.getUserName(), ts,
           Bytes.toBytes(owner))
       .add(FileField.getFileAttributes(), FileField.getGroupName(), ts,
            Bytes.toBytes(group));
  }

  public void replaceBlock(Block last) {
    for(LocatedBlock block : blocks) {
      if(block.getBlock().getBlockId() == last.getBlockId()) {
        block.getBlock().set(last.getBlockId(), last.getNumBytes(), last.getGenerationStamp());
        return;
      }
    }
  }

  public void setBlocks() {
    long ts = now();
    put.add(FileField.getFileAttributes(), FileField.getBlock(), ts,
           getBlocksBytes());
  }

  public void setBlockAction(BlockAction newBlockAction) {
    this.blockAction = newBlockAction;
    long ts = now();
    put.add(FileField.getFileAttributes(), FileField.getAction(), ts,
            Bytes.toBytes(blockAction.toString()));
  }
}
