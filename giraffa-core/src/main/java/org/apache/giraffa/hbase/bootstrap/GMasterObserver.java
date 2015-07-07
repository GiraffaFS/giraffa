package org.apache.giraffa.hbase.bootstrap;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.GMaster;

public class GMasterObserver extends BaseMasterObserver {
  static final Log LOG = LogFactory.getLog(GMasterObserver.class);

  @Override
  public void preMasterInitialization(
      ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    getMaster(ctx).createMetaMetaTable();
    LOG.info("GMasterObserver.preMasterInitialization()");
  }

  @Override
  public void postStartMaster(
      ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    ((BSFileSystem)getMaster(ctx).getMasterFileSystem()
        .getFileSystem()).finalizeBootstrap();
    LOG.info("GMasterObserver.postStartMaster()");
  }

  GMaster getMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) {
    return (GMaster) ctx.getEnvironment().getMasterServices();
  }
}