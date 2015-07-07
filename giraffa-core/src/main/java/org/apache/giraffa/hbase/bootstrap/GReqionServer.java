package org.apache.giraffa.hbase.bootstrap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.master.GMaster.BSFSConfiguration;

public class GReqionServer extends MiniHBaseClusterRegionServer {

  public GReqionServer(Configuration conf, CoordinatedStateManager csm)
      throws IOException, InterruptedException {
    super(new BSFSConfiguration(conf), csm);
  }
}
