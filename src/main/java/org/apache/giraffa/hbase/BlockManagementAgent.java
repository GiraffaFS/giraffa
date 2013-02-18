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
package org.apache.giraffa.hbase;

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FileField;
import org.apache.giraffa.hbase.NamespaceAgent.BlockAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.EnumSetWritable;

/**
 * BlockManagementAgent provides access to underlying block management layer.
 * <p>
 * It is implemented as a HBase coprocessor.
 * When a new block is added to a Giraffa file or the block attribute values
 * need to be revised the update of this information in the namespace table
 * triggers the coprocessor to perform actions on the block management layer.
 * <p>
 * Current implementation uses HDFS NameNode as the block manager.
 * The NameNode maintains a flat namespace of Giraffa blocks as HDFS files.
 * Each Giraffa block is represented by a single-block HDFS file.
 * The name of the file equals the Id of the single HDFS block it contains.
 * 
 * NameNode automatically handles replication of HDFS block and
 * processes heartbeats, block reports, etc., from DataNodes.
 */
public class BlockManagementAgent extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(BlockManagementAgent.class);

  private static final String GRFA_HOME_DIR = "/giraffa";
  private static final String GRFA_BLOCKS_DIR = GRFA_HOME_DIR + "/finalized";
  private static final String GRFA_TMP_BLOCKS_DIR = GRFA_HOME_DIR + "/tmp";
  private static final String GRFA_BLOCK_FILE_PREFFIX = "g";
  private static final String GRFA_TMP_FILE_PREFFIX = "tmp_";

  private DistributedFileSystem hdfs;
  private volatile long temporaryFileId;
  private String clientName;

  @Override // BaseRegionObserver
  public void start(CoprocessorEnvironment e) throws IOException {
    LOG.info("Start BlockManagementAgent...");
    Configuration conf = e.getConfiguration();
    String bmAddress = conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
    LOG.info("BlockManagementAgent address: " + bmAddress);
    if(bmAddress != null)
      conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, bmAddress);
    hdfs = (DistributedFileSystem) DistributedFileSystem.get(conf);
    String msg =null;
    if(!hdfs.mkdirs(new Path(GRFA_BLOCKS_DIR)))
      msg = "Cannot create finalized block directory: " + GRFA_BLOCKS_DIR;
    else if(!hdfs.mkdirs(new Path(GRFA_TMP_BLOCKS_DIR)))
      msg = "Cannot create remporary block directory: " + GRFA_TMP_BLOCKS_DIR;
    if(msg != null) {
      LOG.error(msg);
      throw new IOException(msg);
    }
    temporaryFileId = now();
    clientName = getClientName();
  }

  private String getClientName() {
    String toParse = hdfs.getClient().toString();
    return toParse.substring((toParse.indexOf('='))+1, toParse.lastIndexOf(','));
  }

  @Override // BaseRegionObserver
  public void stop(CoprocessorEnvironment e) {
    /*
    try {
      if(hdfs != null) hdfs.close();
    } catch (IOException exc) {
      LOG.error("DistributedFileSystem could not be closed.");
    }
    hdfs = null;
    */
  }

  @Override // BaseRegionObserver
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    List<KeyValue> kvs = put.getFamilyMap().get(FileField.getFileAttributes());
    BlockAction blockAction = getBlockAction(kvs);
    if(blockAction == null) {
      return;
    } else if(blockAction.equals(BlockAction.ALLOCATE)) {
      allocateBlock(kvs);
    } else if(blockAction.equals(BlockAction.CLOSE)) {
      completeBlocks(kvs);
    } else if(blockAction.equals(BlockAction.DELETE)) {
      deleteBlocks(kvs);
    }
  }

  private void deleteBlocks(List<KeyValue> kvs) {
    // remove the blockAction
    removeBlockAction(kvs);

    ArrayList<LocatedBlock> al = getFileBlocks(kvs);
    for(LocatedBlock block : al) {
      try {
        hdfs.delete(getGiraffaBlockPath(block.getBlock()), true);
      } catch (IOException e) {
        LOG.error("Error deleting Giraffa block: " + block);
        continue;
      }
      LOG.info("Deleted Giraffa block: " + block);
    }
  }

private void removeBlockAction(List<KeyValue> kvs) {
    KeyValue kv = findField(kvs, FileField.ACTION);
    kvs.remove(kv);
}

  static KeyValue findField(List<KeyValue> kvs, FileField field) {
    for(KeyValue kv : kvs) {
      if(kv.matchingColumn(FileField.getFileAttributes(), field.getBytes())) {
        return kv;
      }
    }
    return null;
  }

  static BlockAction getBlockAction(List<KeyValue> kvs) {
    KeyValue kv = findField(kvs, FileField.ACTION);
    return kv == null ? null : 
      BlockAction.valueOf(Bytes.toString(kv.getValue()));
  }

  static ArrayList<LocatedBlock> getFileBlocks(List<KeyValue> kvs) {
    KeyValue kv = findField(kvs, FileField.BLOCK);
    return kv == null ? new ArrayList<LocatedBlock>() :
      byteArrayToBlockList(kv.getValue());
  }

  private void completeBlocks(List<KeyValue> kvs) throws IOException {
    // remove the blockAction
    removeBlockAction(kvs);

    ArrayList<LocatedBlock> al = getFileBlocks(kvs);
    // get the last block
    LocatedBlock block = al.get(al.size()-1);
    closeBlockFile(block.getBlock());
    LOG.info("Block file is closed: " + block);
    // return total fileSize to update in the put
    updateFileSize(kvs, getFileSize(al));
  }

  private void updateFileSize(List<KeyValue> kvs, long fileSize) {
    KeyValue kv = findField(kvs, FileField.LENGTH);
    if(kv == null) return;
    byte[] row = kv.getRow();
    long timestamp = kv.getTimestamp();
    KeyValue nkv = new KeyValue(row,
        FileField.getFileAttributes(), FileField.getLength(),
        timestamp, Bytes.toBytes(fileSize));

    //replace this KeyValue with new KeyValue
    kvs.remove(kv);
    kvs.add(nkv);
    return;
  }

  /**
   * Calls allocateBlockFile to create a new block for the file if and only if
   * we are modifying the Block column in HBase via this put.
   * 
   * @param kvs
   * @throws IOException
   */
  private void allocateBlock(List<KeyValue> kvs) throws IOException {
    // remove the blockAction
    removeBlockAction(kvs);

    KeyValue kv = findField(kvs, FileField.BLOCK);
    if(kv != null) {
      // modifying the block column
      LOG.info("Altering put edits...");
      // create arrayList from this current KeyValue
      ArrayList<LocatedBlock> al = byteArrayToBlockList(kv.getValue());

      LOG.info("al := " + al);
      // get new empty Block and add it to list
      LocatedBlock loc = allocateBlockFile(al);
      al.add(loc);

      // grab old KeyValue data and create new KeyValue
      byte[] row = kv.getRow();
      long timestamp = kv.getTimestamp();
      KeyValue nkv = new KeyValue(row, FileField.getFileAttributes(),
          FileField.getBlock(), timestamp, blockArrayToBytes(al));

      // replace this KeyValue with new KeyValue
      kvs.remove(kv);
      kvs.add(nkv);
    }
  }

  /**
   * When a new block is created, BlockManagementAgent creates a new
   * empty file in HDFS with a unique temporary name.
   * Then it allocates a new single block for that temporary file,
   * obtains its blockId, and renames the temporary file to the name
   * composed of the blockId.
   * 
   * @param blocks
   * @return LocatedBlock
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private LocatedBlock allocateBlockFile(ArrayList<LocatedBlock> blocks)
  throws IOException {
    String tmpFile = getTemporaryBlockPath().toString();

    // create temporary block file
    DFSClient dfsClient = hdfs.getClient();
    dfsClient.getNamenode().create(
            tmpFile, FsPermission.getDefault(), clientName,
            new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
            true, dfsClient.getDefaultReplication(), dfsClient.getDefaultBlockSize());
    // assert tmpOut != null : "File create never returns null";

    // if previous block exists, get it
    Block previous = null;
    if(!blocks.isEmpty()) {
      previous = blocks.get(blocks.size() - 1).getBlock();
      // Close file for the previous block
      closeBlockFile(previous);
      LOG.info("Previous block file is closed: " + previous);
    }

    // add block and close previous
    LocatedBlock block = null;
    block = dfsClient.getNamenode().addBlock(
        tmpFile.toString(), clientName, previous, null);
    // Update block offset
    long offset = getFileSize(blocks);
    block = new LocatedBlock(block.getBlock(), block.getLocations(), offset);

    // rename temporary file to the Giraffa block file
    dfsClient.getNamenode().rename(tmpFile, getGiraffaBlockPath(block.getBlock()).toString());
    LOG.info("Allocated Giraffa block: " + block);
    return block;
  }

  private void closeBlockFile(Block block) throws IOException {
    boolean isClosed = false;
    while(!isClosed) {
      isClosed = hdfs.getClient().getNamenode().complete(
          getGiraffaBlockPathName(block),
          clientName, block);
    }
  }

  static long getFileSize(ArrayList<LocatedBlock> al) {
    long n = 0;
    for(LocatedBlock bl : al) {
      n += bl.getBlockSize();
    }
    return n;
  }

  private Path getTemporaryBlockPath() {
    temporaryFileId++;
    return new Path(GRFA_TMP_BLOCKS_DIR,
        GRFA_TMP_FILE_PREFFIX + temporaryFileId);
  }

  String getGiraffaBlockName(Block block) {
    return GRFA_BLOCK_FILE_PREFFIX + block.getBlockName();
  }

  private String getGiraffaBlockPathName(Block block) {
    return getGiraffaBlockPath(block).toUri().getPath();
  }

  private Path getGiraffaBlockPath(Block block) {
    return new Path(GRFA_BLOCKS_DIR,
        GRFA_BLOCK_FILE_PREFFIX + block.getBlockName());
  }

  /**
   * Convert ArrayList of LocatedBlocks to a byte array.
   * @param blocks
   * @return Byte array representation of array of LocatedBlocks,
   * or null if fails.
   */
  private byte[] blockArrayToBytes(ArrayList<LocatedBlock> blocks) {
    byte[] retVal = null;

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      for(LocatedBlock loc : blocks) {
        loc.write(out);
      }
      retVal = baos.toByteArray();
      out.close();
    } catch (IOException e) {
      return null;
    }
    return retVal;
  }

  /**
   * Convert a byte array into an ArrayList of LocatedBlocks.
   * @param blockArray
   * @return ArrayList representation of byte array of LocatedBlocks,
   *  returns null if fails.
   */
  static ArrayList<LocatedBlock> byteArrayToBlockList(byte[] blockArray) {
    ArrayList<LocatedBlock> locs = new ArrayList<LocatedBlock>();
    DataInputStream in = null;
    try {
      in = new DataInputStream(new ByteArrayInputStream(blockArray));
      while(in.available() > 0) {
        LocatedBlock loc = LocatedBlock.read(in);
        locs.add(loc);
      }
      in.close();
    } catch (IOException e) {
      return null;
    }
    return locs;
  }
}
