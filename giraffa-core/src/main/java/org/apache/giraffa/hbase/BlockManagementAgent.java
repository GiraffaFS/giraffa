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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FileField;
import org.apache.giraffa.FileLease;
import org.apache.giraffa.GiraffaPBHelper;
import org.apache.giraffa.LeaseManager;
import org.apache.giraffa.UnlocatedBlock;
import org.apache.giraffa.hbase.NamespaceAgent.BlockAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HDFSAdapter;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.EnumSetWritable;

import static org.apache.giraffa.GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_TABLE_NAME_KEY;

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
  private AtomicLong temporaryFileId;
  private LeaseManager leaseManager;
  private String clientName;

  @Override // BaseRegionObserver
  public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof RegionCoprocessorEnvironment)) {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
    RegionCoprocessorEnvironment e = (RegionCoprocessorEnvironment) env;
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
    temporaryFileId = new AtomicLong(now());
    clientName = HDFSAdapter.getClientName(hdfs);

    this.leaseManager =
        LeaseManager.getLeaseManager(e.getRegionServerServices());
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

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    RegionCoprocessorEnvironment env = e.getEnvironment();
    HRegion region = env.getRegion();
    Configuration conf = env.getConfiguration();
    if(!isNamespaceTable(region, conf)) {
      return;
    }
    try {
      RegionScanner scanner =
          region.getScanner(new Scan(region.getStartKey(), region.getEndKey()));
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      boolean hasNextRow;
      do {
        kvs.clear();
        hasNextRow = scanner.nextRaw(kvs);
        FileLease lease = getLease(kvs);
        if(lease != null) {
          LOG.info("Migrated FileLease: " + lease);
          leaseManager.addLease(lease);
          // LeaseManager will update lease with a new, higher, expiration date.
          // Until a lease renewal or expiration happens, the lease of the row
          // vs the lease in the LeaseManager will differ in expiration date.
        }
      } while(hasNextRow);
    } catch (IOException exception) {
      LOG.error("Failed to scan INodes cleanly post open of region: " + region,
          exception);
    }
  }

  private boolean isNamespaceTable(HRegion region, Configuration conf) {
    TableName tableName = region.getRegionInfo().getTableName();
    String namespaceTableName = conf.get(GRFA_TABLE_NAME_KEY,
        GRFA_TABLE_NAME_DEFAULT);
    return tableName.getNameAsString().equals(namespaceTableName);
  }

  @Override // BaseRegionObserver
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
                     WALEdit edit, Durability durability) throws IOException {
    List<KeyValue> kvs = getKeyValues(put);
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
    put.getFamilyCellMap().put(FileField.getFileAttributes(),
        new ArrayList<Cell>(kvs));
  }

  private List<KeyValue> getKeyValues(Put put) {
    List<Cell> cells =
        put.getFamilyCellMap().get(FileField.getFileAttributes());
    List<KeyValue> kvs = new ArrayList<KeyValue>(cells.size());
    for(Cell cell : cells) {
      kvs.add(KeyValueUtil.ensureKeyValue(cell));
    }
    return kvs;
  }

  private void deleteBlocks(List<KeyValue> kvs) {
    // remove the blockAction
    removeBlockAction(kvs);

    List<UnlocatedBlock> al = getFileBlocks(kvs);
    for(UnlocatedBlock block : al) {
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

  static List<UnlocatedBlock> getFileBlocks(List<KeyValue> kvs) {
    KeyValue kv = findField(kvs, FileField.BLOCK);
    return kv == null ? new ArrayList<UnlocatedBlock>() :
      byteArrayToBlockList(kv.getValue());
  }

  static FileLease getLease(List<KeyValue> kvs) {
    KeyValue kv = findField(kvs, FileField.LEASE);
    return kv == null ? null : byteArrayToLease(kv.getValue());
  }

  private void completeBlocks(List<KeyValue> kvs) throws IOException {
    // remove the blockAction
    removeBlockAction(kvs);

    List<UnlocatedBlock> al = getFileBlocks(kvs);
    // get the last block
    UnlocatedBlock block = al.get(al.size()-1);
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
  }

  /**
   * Calls allocateBlockFile to create a new block for the file if and only if
   * we are modifying the Block column in HBase via this put.
   * 
   *
   * @param kvs
   * @throws IOException
   */
  private void allocateBlock(List<KeyValue> kvs) throws IOException {
    // remove the blockAction
    removeBlockAction(kvs);

    KeyValue blockKv = findField(kvs, FileField.BLOCK);
    KeyValue locsKv = findField(kvs, FileField.LOCATIONS);
    if(blockKv != null && locsKv != null) {
      // create arrayLists from current KeyValues
      List<UnlocatedBlock> al_blks =
          byteArrayToBlockList(blockKv.getValue());
      List<DatanodeInfo[]> al_locs =
          byteArrayToLocsList(locsKv.getValue());

      // get new empty Block, seperate into blocks/locations, and add to lists
      LocatedBlock locatedBlock = allocateBlockFile(al_blks);
      UnlocatedBlock blk = new UnlocatedBlock(locatedBlock);
      DatanodeInfo[] locs = locatedBlock.getLocations();
      al_blks.add(blk);
      al_locs.add(locs);

      // grab old KeyValue data and create new KeyValue for blocks
      byte[] row = blockKv.getRow();
      long blk_timestamp = blockKv.getTimestamp();
      KeyValue blockNkv = new KeyValue(row, FileField.getFileAttributes(),
          FileField.getBlock(), blk_timestamp, blockArrayToBytes(al_blks));
      
      // repeat for locations
      long loc_timestamp = locsKv.getTimestamp();
      KeyValue locsNkv = new KeyValue(row, FileField.getFileAttributes(),
          FileField.getLocations(), loc_timestamp, locsArrayToBytes(al_locs));

      // replace original KeyValues with new KeyValues
      kvs.remove(blockKv);
      kvs.remove(locsKv);
      kvs.add(blockNkv);
      kvs.add(locsNkv);

      LOG.info("File: " + kvs.get(0).getKeyString() + " Blocks: " +
          getFileBlocks(kvs));
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
  private LocatedBlock allocateBlockFile(List<UnlocatedBlock> blocks)
  throws IOException {
    String tmpFile = getTemporaryBlockPath().toString();

    ClientProtocol namenode = HDFSAdapter.getClientProtocol(hdfs);
    // create temporary block file
    namenode.create(
            tmpFile, FsPermission.getDefault(), clientName,
            new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
            true,
            hdfs.getDefaultReplication(),
            hdfs.getDefaultBlockSize());

    // if previous block exists, get it
    ExtendedBlock previous = null;
    if(!blocks.isEmpty()) {
      previous = blocks.get(blocks.size() - 1).getBlock();
      // Close file for the previous block
      closeBlockFile(previous);
      LOG.info("Previous block file is closed: " + previous);
    }

    // add block and close previous
    LocatedBlock block = null;
    block = namenode.addBlock(
        tmpFile.toString(), clientName, null, null);
    // Update block offset
    long offset = getFileSize(blocks);
    block = new LocatedBlock(block.getBlock(), block.getLocations(), offset);

    // rename temporary file to the Giraffa block file
    namenode.rename(tmpFile, getGiraffaBlockPath(block.getBlock()).toString());
    LOG.info("Allocated Giraffa block: " + block);
    return block;
  }

  private void closeBlockFile(ExtendedBlock block) throws IOException {
    boolean isClosed = false;
    while(!isClosed) {
      isClosed = HDFSAdapter.getClientProtocol(hdfs).complete(
          getGiraffaBlockPathName(block),
          clientName, block);
    }
  }

  static long getFileSize(List<UnlocatedBlock> al) {
    long n = 0;
    for(UnlocatedBlock bl : al) {
      n += bl.getBlockSize();
    }
    return n;
  }

  private Path getTemporaryBlockPath() {
    return new Path(GRFA_TMP_BLOCKS_DIR,
        GRFA_TMP_FILE_PREFFIX + temporaryFileId.incrementAndGet());
  }

  String getGiraffaBlockName(Block block) {
    return GRFA_BLOCK_FILE_PREFFIX + block.getBlockName();
  }

  private String getGiraffaBlockPathName(ExtendedBlock block) {
    return getGiraffaBlockPath(block).toUri().getPath();
  }

  private Path getGiraffaBlockPath(ExtendedBlock block) {
    return new Path(GRFA_BLOCKS_DIR,
        GRFA_BLOCK_FILE_PREFFIX + block.getBlockName());
  }

  /**
   * Convert ArrayList of UnlocatedBlocks to a byte array.
   * @param blocks
   * @return Byte array representation of array of UnlocatedBlocks,
   * or null if fails.
   */
  private byte[] blockArrayToBytes(List<UnlocatedBlock> blocks) {
    try {
      return GiraffaPBHelper.unlocatedBlocksToBytes(blocks);
    } catch (IOException e) {
      LOG.info("Error with serialization!", e);
    }
    return null;
  }
  
  /**
   * Convert ArrayList of DatanodeInfo[]s to a byte array.
   * @param locations
   * @return Byte array representation of array of DatanodeInfo[]s,
   * or null if fails.
   */
  private byte[] locsArrayToBytes(List<DatanodeInfo[]> locations) {
    try {
      return GiraffaPBHelper.blockLocationsToBytes(locations);
    } catch (IOException e) {
      LOG.info("Error with serialization!", e);
    }
    return null;
  }

  /**
   * Convert a byte array into an ArrayList of UnlocatedBlocks.
   * @param blockArray
   * @return ArrayList representation of byte array of UnlocatedBlocks,
   *  returns null if fails.
   */
  static List<UnlocatedBlock> byteArrayToBlockList(byte[] blockArray) {
    try {
      return GiraffaPBHelper.bytesToUnlocatedBlocks(blockArray);
    } catch (IOException e) {
      LOG.info("Error with serialization!", e);
    }
    return null;
  }
  
  /**
   * Convert a byte array into an ArrayList of DatanodeInfo[]s.
   * @param locsArray
   * @return ArrayList representation of byte array of DatanodeInfo[]s,
   *  returns null if fails.
   */
  static List<DatanodeInfo[]> byteArrayToLocsList(byte[] locsArray) {
    try {
      return GiraffaPBHelper.bytesToBlockLocations(locsArray);
    } catch (IOException e) {
      LOG.info("Error with serialization!", e);
    }
    return null;
  }

  /**
   * Convert a byte array into a FileLease.
   * @param lease
   * @return FileLease representation of byte array of FileLease,
   *  returns null if fails.
   */
  static FileLease byteArrayToLease(byte[] lease) {
    try {
      return GiraffaPBHelper.bytesToHdfsLease(lease);
    } catch (IOException e) {
      LOG.info("Error with serialization!", e);
    }
    return null;
  }

  private static long now() {
    return System.currentTimeMillis();
  }
}
