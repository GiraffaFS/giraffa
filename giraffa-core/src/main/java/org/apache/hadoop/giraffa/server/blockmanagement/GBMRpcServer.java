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
package org.apache.hadoop.giraffa.server.blockmanagement;

import com.google.protobuf.BlockingService;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_GBM_HANDLER_COUNT_DEFAULT;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_GBM_HANDLER_COUNT_KEY;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_GBM_SERVICE_RPC_ADDRESS_DEFAULT;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_GBM_SERVICE_RPC_BIND_HOST_KEY;
import org.apache.giraffa.UnlocatedBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.giraffa.protocol.GBMNodeProtocols;
import org.apache.hadoop.giraffa.protocol.proto.GBMClientProtocolProtos.GBMClientProtocolService;
import org.apache.hadoop.giraffa.protocolPB.GBMClientProtocolPB;
import org.apache.hadoop.giraffa.protocolPB.GBMClientProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

/**
 * This class is responsible for handling all of the RPC calls to
 * GiraffaBlockManagement server.
 */
class GBMRpcServer implements GBMNodeProtocols {

  private static final Log LOG = LogFactory.getLog(GBMRpcServer.class.getName());

  private static final String GRFA_HOME_DIR = "/giraffa";
  private static final String GRFA_BLOCKS_DIR = GRFA_HOME_DIR + "/finalized";
  private static final String GRFA_TMP_BLOCKS_DIR = GRFA_HOME_DIR + "/tmp";
  private static final String GRFA_BLOCK_FILE_PREFFIX = "g";
  private static final String GRFA_TMP_FILE_PREFFIX = "tmp_";

  /**
   * The RPC server that listens to requests from clients
   */
  private final RPC.Server clientRpcServer;

  private final NameNode namenode;
  private final DistributedFileSystem dfs;

  public GBMRpcServer(Configuration conf, NameNode namenode)
          throws IOException {
    this.namenode = namenode;
    this.dfs = (DistributedFileSystem) DistributedFileSystem.get(conf);

    RPC.setProtocolEngine(conf, GBMClientProtocolPB.class,
            ProtobufRpcEngine.class);
    GBMClientProtocolServerSideTranslatorPB bmProtocolServerTranslator
            = new GBMClientProtocolServerSideTranslatorPB(this);
    BlockingService blockManagementPbService = GBMClientProtocolService.
            newReflectiveBlockingService(bmProtocolServerTranslator);

    InetSocketAddress addr = NetUtils.createSocketAddr(
            GRFA_GBM_SERVICE_RPC_ADDRESS_DEFAULT);
    this.clientRpcServer = new RPC.Builder(conf)
            .setProtocol(GBMClientProtocolPB.class)
            .setInstance(blockManagementPbService)
            .setBindAddress(conf.getTrimmed(GRFA_GBM_SERVICE_RPC_BIND_HOST_KEY,
                            addr.getHostString()))
            .setPort(addr.getPort())
            .setNumHandlers(conf.getInt(GRFA_GBM_HANDLER_COUNT_KEY,
                            GRFA_GBM_HANDLER_COUNT_DEFAULT))
            .setVerbose(false)
            .build();
  }

  @Override
  public LocatedBlock allocateBlockFile(String src, List<UnlocatedBlock> blocks,
          String clientName) throws IOException {
    ClientProtocol nnClient = namenode.getRpcServer();

    // create temporary block file
    HdfsFileStatus tmpOut = nnClient.create(
            src, FsPermission.getDefault(), clientName,
            new EnumSetWritable<>(EnumSet.of(CreateFlag.CREATE)),
            true,
            dfs.getDefaultReplication(),
            dfs.getDefaultBlockSize());
    assert tmpOut != null : "File create never returns null";

    // if previous block exists, get it
    ExtendedBlock previous;
    if (!blocks.isEmpty()) {
      previous = blocks.get(blocks.size() - 1).getBlock();
      // Close file for the previous block
      closeBlockFile(previous, clientName);
      LOG.info("Previous block file is closed: " + previous);
    }

    // add block and close previous
    LocatedBlock block = nnClient.addBlock(src, clientName, null, null,
            tmpOut.getFileId(), null);
    // Update block offset
    long offset = getFileSize(blocks);
    block = new LocatedBlock(block.getBlock(), block.getLocations(), offset,
            false);

    // rename temporary file to the Giraffa block file
    nnClient.rename(src, getGiraffaBlockPath(block.getBlock()).toString());
    LOG.info("Allocated Giraffa block: " + block);
    return block;
  }

  @Override
  public void closeBlockFile(ExtendedBlock block, String clientName) throws
          IOException {
    ClientProtocol nnClient = namenode.getRpcServer();
    boolean isClosed = false;
    while (!isClosed) {
      isClosed = nnClient.complete(getGiraffaBlockPathName(block), clientName,
              block, INodeId.GRANDFATHER_INODE_ID);
    }
  }

  private Path getGiraffaBlockPath(ExtendedBlock block) {
    return new Path(GRFA_BLOCKS_DIR,
            GRFA_BLOCK_FILE_PREFFIX + block.getBlockName());
  }

  private String getGiraffaBlockPathName(ExtendedBlock block) {
    return getGiraffaBlockPath(block).toUri().getPath();
  }

  private static long getFileSize(List<UnlocatedBlock> al) {
    long n = 0;
    for (UnlocatedBlock bl : al) {
      n += bl.getBlockSize();
    }
    return n;
  }

  /**
   * Start client RPC server.
   */
  void start() {
    clientRpcServer.start();
  }

  /**
   * Wait until the RPC server has shutdown.
   */
  void join() throws InterruptedException {
    clientRpcServer.join();
  }

  /**
   * Stop client RPC server.
   */
  void stop() {
    if (clientRpcServer != null) {
      clientRpcServer.stop();
    }
  }

}
