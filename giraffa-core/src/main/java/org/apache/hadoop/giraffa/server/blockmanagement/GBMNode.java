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

import java.io.IOException;
import java.util.concurrent.Executors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import static org.apache.hadoop.hdfs.server.namenode.NameNode.createNameNode;
import static org.apache.hadoop.util.ExitUtil.terminate;
import org.apache.hadoop.util.StringUtils;

/**
 * Giraffa's BlockManagementNode serves as a manager for low-level block
 * operations. It works in pair with HBase region server to provide locality of
 * the blocks belonging to the files stored in a local region.
 */
public class GBMNode {

  private static final Log LOG = LogFactory.getLog(GBMNode.class.
          getName());  
  
  // TODO: Remove NameNode usage when NameNode dependency removed.
  private static final String NAMENODE_USAGE = "Usage: java NameNode ["
      + HdfsServerConstants.StartupOption.BACKUP.getName() + "] | \n\t["
      + HdfsServerConstants.StartupOption.CHECKPOINT.getName() + "] | \n\t["
      + HdfsServerConstants.StartupOption.FORMAT.getName() + " ["
      + HdfsServerConstants.StartupOption.CLUSTERID.getName() + " cid ] ["
      + HdfsServerConstants.StartupOption.FORCE.getName() + "] ["
      + HdfsServerConstants.StartupOption.NONINTERACTIVE.getName() + "] ] | \n\t["
      + HdfsServerConstants.StartupOption.UPGRADE.getName() + 
        " [" + HdfsServerConstants.StartupOption.CLUSTERID.getName() + " cid]" +
        " [" + HdfsServerConstants.StartupOption.RENAMERESERVED.getName() + "<k-v pairs>] ] | \n\t["
      + HdfsServerConstants.StartupOption.ROLLBACK.getName() + "] | \n\t["
      + HdfsServerConstants.StartupOption.ROLLINGUPGRADE.getName() + " <"
      + HdfsServerConstants.RollingUpgradeStartupOption.DOWNGRADE.name().toLowerCase() + "|"
      + HdfsServerConstants.RollingUpgradeStartupOption.ROLLBACK.name().toLowerCase() + "> ] | \n\t["
      + HdfsServerConstants.StartupOption.FINALIZE.getName() + "] | \n\t["
      + HdfsServerConstants.StartupOption.IMPORT.getName() + "] | \n\t["
      + HdfsServerConstants.StartupOption.INITIALIZESHAREDEDITS.getName() + "] | \n\t["
      + HdfsServerConstants.StartupOption.BOOTSTRAPSTANDBY.getName() + "] | \n\t["
      + HdfsServerConstants.StartupOption.RECOVER.getName() + " [ "
      + HdfsServerConstants.StartupOption.FORCE.getName() + "] ] | \n\t["
      + HdfsServerConstants.StartupOption.METADATAVERSION.getName() + " ] "
      + " ]";
  
  private final GBMRpcServer rpcServer;

  public GBMNode(Configuration conf, NameNode nameNode) throws IOException {
    try {
      rpcServer = new GBMRpcServer(conf, nameNode);
    } catch (IOException e) {
      this.stopNode();
      throw e;
    }
    rpcServer.start();
  }

  private void stopNode() {
    synchronized (this) {
      if (rpcServer != null) {
        rpcServer.stop();
      }      
    }
  }

  public void stop() {
    stopNode();
  }

  public void join() {
    try {
      rpcServer.join();
    } catch (InterruptedException ie) {
      LOG.info("Caught interrupted exception ", ie);
    }
  }

  public static GBMNode createGBMNode(String[] args,
          Configuration conf, NameNode nameNode) throws IOException {
    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    return new GBMNode(conf, nameNode);
  }

  public static void main(String[] args) throws Exception {
    // Create NameNode
    NameNode nameNode = null;
    if (DFSUtil.parseHelpArgument(args, GBMNode.NAMENODE_USAGE, System.out,
            true)) {
      System.exit(0);
    }
    try {
      StringUtils.startupShutdownMessage(NameNode.class, args, LOG);
      nameNode = createNameNode(args, null);
    } catch (Throwable e) {
      LOG.fatal("Exception while creating namenode", e);
      terminate(1, e);
    }

    // Create GBMNode
    GBMNode gbmNode = null;
    try {
      StringUtils.startupShutdownMessage(GBMNode.class, args, LOG);
      gbmNode = createGBMNode(args, null, nameNode);
    } catch (Throwable e) {
      LOG.fatal("Exception while creating GBMNode", e);
      terminate(1, e);
    }

    // Run nodes
    Executors.newSingleThreadExecutor().submit(new NameNodeRunner(nameNode));
    try {
      if (gbmNode != null) {
        gbmNode.join();
      }
    } catch (Exception e) {
      LOG.fatal("Exception in GBMNode join", e);
    }
  }

  /**
   * It helps NameNode to join in a separate thread.
   */
  private static class NameNodeRunner implements Runnable {

    private final NameNode namenode;

    public NameNodeRunner(NameNode namenode) {
      this.namenode = namenode;
    }

    @Override
    public void run() {
      try {
        namenode.join();
      } catch (Exception e) {
        LOG.fatal("Exception in namenode join", e);
      }
    }
  }

}
