package org.apache.giraffa.hbase.bootstrap;

import static org.apache.giraffa.GiraffaConfiguration.GRFA_BLOCK_MANAGER_ADDRESS_KEY;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_BOOTSTRAP_FS_IMPL;
import static org.apache.hadoop.hbase.HConstants.HBASE_CHECKSUM_VERIFICATION;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.util.FSUtils;

public class BSFSConfiguration extends Configuration {
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
    this.setBoolean("fs.bsfs.impl.disable.cache", true);
    // Allow meta table on the master (SHV !! check if it works)
    this.set(BaseLoadBalancer.TABLES_ON_MASTER,
             TableName.META_TABLE_NAME.getNameAsString());
    // Turn off HBase checksums
    this.setBoolean(HBASE_CHECKSUM_VERIFICATION, false);
    // Add observers to support bootstrap
    this.set(REGIONSERVER_COPROCESSOR_CONF_KEY,
             GRegionServerObserver.class.getCanonicalName());
    this.set(MASTER_COPROCESSOR_CONF_KEY,
             GMasterObserver.class.getCanonicalName());
  }
}