package org.apache.hadoop.hbase.master;

import static org.apache.giraffa.GiraffaConfiguration.GRFA_BLOCK_MANAGER_ADDRESS_KEY;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_BOOTSTRAP_FS_IMPL;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.bootstrap.BSFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;

public class HMasterGiraffa extends HMaster {
  private static final Log LOG = LogFactory.getLog(HMasterGiraffa.class);

  public HMasterGiraffa(Configuration conf, CoordinatedStateManager csm)
      throws IOException, KeeperException, InterruptedException {
    super(new BSFSConfiguration(conf), csm);
    // ((BSFileSystem)getMasterFileSystem().getFileSystem()).setBootStrapped();
    LOG.info(this.getClass().getCanonicalName() + " is instantiated");
  }

  public static class BSFSConfiguration extends Configuration {
    public BSFSConfiguration(Configuration conf) throws IOException {
      super(conf);
      String bmAddress = conf.get(GRFA_BLOCK_MANAGER_ADDRESS_KEY);
      if(bmAddress == null) {
        bmAddress = conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
        bmAddress = bmAddress.replaceAll("bsfs:", "hdfs:");
        this.set(GRFA_BLOCK_MANAGER_ADDRESS_KEY, bmAddress);
        conf.set(GRFA_BLOCK_MANAGER_ADDRESS_KEY, bmAddress);
      }
      Path hbaseRoot = new Path(conf.get(HConstants.HBASE_DIR));
      Path root = new Path("bsfs", hbaseRoot.toUri().getAuthority(),
                                   hbaseRoot.toUri().getPath());
      FSUtils.setFsDefault(this, root);
      FSUtils.setRootDir(this, root);
      this.set("fs.bsfs.impl", GRFA_BOOTSTRAP_FS_IMPL);
      // Allow meta table on the master
      this.set(BaseLoadBalancer.TABLES_ON_MASTER,
               TableName.META_TABLE_NAME.getNameAsString());
    }
  }

  /**
   * Finalize bootstrap file system before opening the connections.
   */
  @Override // HMaster
  // protected void setupClusterConnection() throws IOException {
  void assignMeta(MonitoredTask status, Set<ServerName> previouslyFailedMetaRSs)
      throws InterruptedException, IOException, KeeperException {
    FileSystem fs = getMasterFileSystem().getFileSystem();
    // save into super block
    if(!(fs instanceof BSFileSystem))
      throw new IOException("Not a bootstrap fs: "
          + fs.getClass().getCanonicalName());
    ((BSFileSystem)fs).writeSuperBlock(conf);

    // call super
    super.assignMeta(status, previouslyFailedMetaRSs);
 // super.setupClusterConnection();
  }
}
