package org.apache.giraffa.hbase.bootstrap;

import static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName;
import static org.apache.hadoop.util.Time.now;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;

public class GReqionServer extends MiniHBaseClusterRegionServer {

  public GReqionServer(Configuration conf, CoordinatedStateManager csm)
      throws IOException, InterruptedException {
    super(new BSFSConfiguration(conf), csm);
  }

  // SHV !! Should be done as a procedure: RegionServerProcedureManagerHost
  // Configure Giraffa startup ProcedureManager to wait for Namespace table
  @Override // HRegionServer
  protected void handleReportForDutyResponse(final RegionServerStartupResponse c)
  throws IOException {
    LOG.info("GReqionServer.handleReportForDutyResponse()");
    super.handleReportForDutyResponse(c);
    waitMetaMetaTable();
  }

  void waitMetaMetaTable() throws IOException {
    TableName tName = TableName.valueOf(getGiraffaTableName(conf));
    Table mmTable = null;

    LOG.debug("Waiting for Namespace table");
    long timeout = 300000; // SHV !! TableNamespaceManager.DEFAULT_NS_INIT_TIMEOUT
    long startTime = now();
    while((mmTable = ((Connection)getConnection()).getTable(tName)) == null) {
      if(now() - startTime + 100 > timeout) {
        // We can't do anything if ns is not online.
        throw new IOException("Timedout " + timeout + "ms waiting for " +
            tName + " table to be created.");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted creating table.", e);
      }
    }
    HTableDescriptor htd = mmTable.getTableDescriptor();
    LOG.debug("Namespace table found: " + htd);

    BSFileSystem bsfs = (BSFileSystem)((HFileSystem)getFileSystem()).getBackingFs();
    bsfs.finalizeBootstrap();
  }
}
