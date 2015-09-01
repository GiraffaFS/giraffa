package org.apache.giraffa.hbase.bootstrap;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

public class GRegionServerObserver extends BaseRegionServerObserver {
  static final Log LOG = LogFactory.getLog(GRegionServerObserver.class);

  public GRegionServerObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) {
    LOG.info("GRegionServerObserver.start()");
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    LOG.info("GRegionServerObserver.stop()");
  }

  @Override
  public void preStopRegionServer(
      ObserverContext<RegionServerCoprocessorEnvironment> env)
          throws IOException {
    LOG.info("GRegionServerObserver.preStopRegionServer()");
    for(HRegion r: ((HRegionServer) env.getEnvironment().getRegionServerServices()).getOnlineRegionsLocalContext()) {
      r.flushcache();
    }
  }
}
