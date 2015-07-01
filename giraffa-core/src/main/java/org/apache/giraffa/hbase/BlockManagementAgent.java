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

import static org.apache.giraffa.GiraffaConstants.FileState;
import static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName;
import static org.apache.hadoop.hbase.CellUtil.matchingColumn;
import static org.apache.hadoop.util.Time.now;

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
import org.apache.giraffa.GiraffaConstants.BlockAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
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
        LeaseManager.originateSharedLeaseManager(e.getRegionServerServices()
            .getRpcServer().getListenerAddress().toString());
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
      List<Cell> cells = new ArrayList<Cell>();
      boolean hasNextRow;
      do {
        cells.clear();
        hasNextRow = scanner.nextRaw(cells);
        FileLease lease = getFileLease(cells);
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

  static boolean isNamespaceTable(HRegion region, Configuration conf) {
    TableName tableName = region.getRegionInfo().getTable();
    String namespaceTableName = getGiraffaTableName(conf);
    return tableName.getNameAsString().equals(namespaceTableName);
  }

  @Override // BaseRegionObserver
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
                     WALEdit edit, Durability durability) throws IOException {
    List<Cell> kvs = put.getFamilyCellMap().get(FileField.getFileAttributes());

    // If we want to do XAttr related attribute, the CF is different
    // so it will get NULL
    if (kvs == null) { return; }

    BlockAction blockAction = getBlockAction(kvs);
    if(blockAction == null) {
      return;
    }
    switch (blockAction) {
      case ALLOCATE:
        allocateBlock(kvs);
        break;
      case DELETE:
        deleteBlocks(kvs);
        break;
      case CLOSE:
        completeBlocks(kvs);
        break;
      case RECOVER:
        recoverLastBlock(kvs);
        break;
      default:
        LOG.debug("Unknown BlockAction found: " + blockAction + ", returning.");
        return;
    }
    put.getFamilyCellMap().put(FileField.getFileAttributes(),
        new ArrayList<Cell>(kvs));
  }

  private void deleteBlocks(List<Cell> kvs) {
    // remove the blockAction
    removeField(kvs, FileField.ACTION);

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

  private void removeField(List<Cell> kvs, FileField field) {
    Cell kv = findField(kvs, field);
    if(kv != null)
      kvs.remove(kv);
  }

  static Cell findField(List<Cell> kvs, FileField field) {
    for(Cell kv : kvs) {
      if(matchingColumn(kv, FileField.getFileAttributes(), field.getBytes())) {
        return kv;
      }
    }
    return null;
  }

  private void updateField(List<Cell> kvs, FileField field, byte[] value) {
    Cell kv = findField(kvs, field);
    if(kv == null) return;
    byte[] row = CellUtil.cloneRow(kv);
    long timestamp = kv.getTimestamp();
    Cell nkv = new KeyValue(row,
        FileField.getFileAttributes(), field.getBytes(),
        timestamp, value);

    //replace this Cell with new Cell
    kvs.remove(kv);
    kvs.add(nkv);
  }

  static BlockAction getBlockAction(List<Cell> kvs) {
    Cell kv = findField(kvs, FileField.ACTION);
    return kv == null ? null : 
      BlockAction.valueOf(Bytes.toString(CellUtil.cloneValue(kv)));
  }

  static List<UnlocatedBlock> getFileBlocks(List<Cell> kvs) {
    Cell kv = findField(kvs, FileField.BLOCK);
    return kv == null ? new ArrayList<UnlocatedBlock>() :
      byteArrayToBlockList(CellUtil.cloneValue(kv));
  }

  static FileLease getFileLease(List<Cell> kvs) throws IOException {
    Cell kv = findField(kvs, FileField.LEASE);
    return kv == null ? null :
      GiraffaPBHelper.bytesToHdfsLease(CellUtil.cloneValue(kv));
  }

  private void completeBlocks(List<Cell> kvs) throws IOException {
    // remove the blockAction
    removeField(kvs, FileField.ACTION);

    List<UnlocatedBlock> al = getFileBlocks(kvs);
    // get the last block
    UnlocatedBlock block = al.get(al.size()-1);
    closeBlockFile(block.getBlock());
    LOG.info("Block file is closed: " + block);
    // return total fileSize to update in the put
    updateField(kvs, FileField.LENGTH, Bytes.toBytes(getFileSize(al)));
  }

  /**
   * Calls allocateBlockFile to create a new block for the file if and only if
   * we are modifying the Block column in HBase via this put.
   * 
   *
   * @param kvs
   * @throws IOException
   */
  private void allocateBlock(List<Cell> kvs) throws IOException {
    // remove the blockAction
    removeField(kvs, FileField.ACTION);

    Cell blockKv = findField(kvs, FileField.BLOCK);
    Cell locsKv = findField(kvs, FileField.LOCATIONS);
    if(blockKv != null && locsKv != null) {
      // create arrayLists from current KeyValues
      List<UnlocatedBlock> al_blks =
          byteArrayToBlockList(CellUtil.cloneValue(blockKv));
      List<DatanodeInfo[]> al_locs =
          byteArrayToLocsList(CellUtil.cloneValue(locsKv));

      // get new empty Block, seperate into blocks/locations, and add to lists
      LocatedBlock locatedBlock = allocateBlockFile(al_blks);
      UnlocatedBlock blk = new UnlocatedBlock(locatedBlock);
      DatanodeInfo[] locs = locatedBlock.getLocations();
      al_blks.add(blk);
      al_locs.add(locs);

      // grab old Cell data and create new Cell for blocks
      byte[] row = CellUtil.cloneRow(blockKv);
      long blk_timestamp = blockKv.getTimestamp();
      Cell blockNkv = new KeyValue(row, FileField.getFileAttributes(),
          FileField.getBlock(), blk_timestamp, blockArrayToBytes(al_blks));
      
      // repeat for locations
      long loc_timestamp = locsKv.getTimestamp();
      Cell locsNkv = new KeyValue(row, FileField.getFileAttributes(),
          FileField.getLocations(), loc_timestamp, locsArrayToBytes(al_locs));

      // replace original KeyValues with new KeyValues
      kvs.remove(blockKv);
      kvs.remove(locsKv);
      kvs.add(blockNkv);
      kvs.add(locsNkv);

      LOG.info("File: " + CellUtil.getCellKeyAsString(kvs.get(0))
          + " Blocks: " + getFileBlocks(kvs));
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
    HdfsFileStatus tmpOut = namenode.create(
        tmpFile, FsPermission.getDefault(), clientName,
        new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
        true,
        hdfs.getDefaultReplication(),
        hdfs.getDefaultBlockSize());
    assert tmpOut != null : "File create never returns null";

    // if previous block exists, get it
    ExtendedBlock previous = null;
    if(!blocks.isEmpty()) {
      previous = blocks.get(blocks.size() - 1).getBlock();
      // Close file for the previous block
      closeBlockFile(previous);
      LOG.info("Previous block file is closed: " + previous);
    }

    // add block and close previous
    LocatedBlock block = namenode.addBlock(tmpFile, clientName, null, null,
        tmpOut.getFileId(), null);
    // Update block offset
    long offset = getFileSize(blocks);
    block = new LocatedBlock(block.getBlock(), block.getLocations(), offset,
        false);

    // rename temporary file to the Giraffa block file
    namenode.rename(tmpFile, getGiraffaBlockPath(block.getBlock()).toString());
    LOG.info("Allocated Giraffa block: " + block);
    return block;
  }

  private void closeBlockFile(ExtendedBlock block) throws IOException {
    boolean isClosed = false;
    while(!isClosed) {
      isClosed = HDFSAdapter.getClientProtocol(hdfs).complete(
          getGiraffaBlockPathName(block), clientName, block,
          INodeId.GRANDFATHER_INODE_ID);
    }
  }

  /**
   * Calls allocateBlockFile to create a new block for the file if and only if
   * we are modifying the Block column in HBase via this put.
   */
  private void recoverLastBlock(List<Cell> kvs) throws IOException {
    LOG.info("Starting block recovery.");
    Cell leaseKv = findField(kvs, FileField.LEASE);
    Cell blockKv = findField(kvs, FileField.BLOCK);
    byte[] leaseBytes = CellUtil.cloneValue(leaseKv);
    byte[] blockBytes = CellUtil.cloneValue(blockKv);

    List<UnlocatedBlock> unlocatedBlocks = byteArrayToBlockList(blockBytes);
    removeField(kvs, FileField.ACTION);

    if(leaseBytes == null || leaseBytes.length == 0) {
      LOG.warn("Could not perform recovery due to missing lease field.");
      return;
    }
    if(blockBytes == null || blockBytes.length == 0) {
      LOG.warn("Could not perform recovery due to missing blocks field.");
      return;
    }
    if(unlocatedBlocks == null || unlocatedBlocks.size() == 0) {
      LOG.warn("Could not perform recovery due to zero blocks in file.");
      return;
    }

    FileLease lease = GiraffaPBHelper.bytesToHdfsLease(leaseBytes);
    ExtendedBlock lastBlock =
        unlocatedBlocks.get(unlocatedBlocks.size() - 1).getBlock();
    String blockFileName = getGiraffaBlockPathName(lastBlock);
    boolean recovered = recoverBlockFile(lastBlock);
    removeField(kvs, FileField.ACTION);
    if(!recovered) {
      LOG.error("Block could not be recovered. File is still under recovery.");
      return;
    } else {
      LOG.info("Recovered block file: " + blockFileName);
    }

    HdfsFileStatus fileInfo = hdfs.getClient().getFileInfo(blockFileName);
    leaseManager.removeLease(lease);

    // update cell values
    updateField(kvs, FileField.LEASE, null);
    updateField(kvs, FileField.LENGTH, Bytes.toBytes(fileInfo.getLen()));
    updateField(kvs, FileField.FILE_STATE,
        Bytes.toBytes(FileState.CLOSED.toString()));
  }

  private boolean recoverBlockFile(ExtendedBlock block) throws IOException {
    String src = getGiraffaBlockPathName(block);
    boolean isRecovered =
        HDFSAdapter.getClientProtocol(hdfs).recoverLease(src, clientName);
    for(int i = 0; i < 100 && !isRecovered; i++) {
      try { Thread.sleep(100L); } catch (InterruptedException ignored) {}
      isRecovered = HDFSAdapter.getClientProtocol(hdfs).isFileClosed(src);
    }
    return isRecovered;
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
}
