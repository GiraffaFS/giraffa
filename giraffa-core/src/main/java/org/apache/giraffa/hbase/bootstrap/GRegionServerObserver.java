package org.apache.giraffa.hbase.bootstrap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;

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
}
