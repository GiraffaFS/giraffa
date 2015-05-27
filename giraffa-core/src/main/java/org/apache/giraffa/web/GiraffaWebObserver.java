package org.apache.giraffa.web;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.GiraffaConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.net.NetUtils;

import javax.servlet.jsp.jstl.core.Config;
import java.io.IOException;
import java.net.InetSocketAddress;

public class GiraffaWebObserver extends BaseMasterObserver {

  private static final Log LOG = LogFactory.getLog(GiraffaWebObserver.class);

  // server for the web ui
  private GiraffaWebServer giraffaServer;

  private InetSocketAddress getHttpServerAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.get(GiraffaConfiguration.GRFA_WEB_ADDRESS_KEY,
            GiraffaConfiguration.GRFA_WEB_ADDRESS_DEFAULT));
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    super.postStartMaster(ctx);

    LOG.info("Start GiraffaWebObserver...");
    Configuration conf = ctx.getEnvironment().getConfiguration();
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin hBaseAdmin = connection.getAdmin();

    final InetSocketAddress infoSocAddr = getHttpServerAddress(conf);

    this.giraffaServer = new GiraffaWebServer("giraffa", infoSocAddr, conf);
    this.giraffaServer.addServlet("files", "/grfa/*", GiraffaFileServlet.class);
    this.giraffaServer.addServlet("hbase", "/hbase/*", GiraffaHbaseServlet.class);
    this.giraffaServer.setAttribute(Config.FMT_LOCALIZATION_CONTEXT + ".application",
             "org.apache.giraffa.web.Messages");
    this.giraffaServer.setAttribute("conf", conf);
    this.giraffaServer.setAttribute("hBaseAdmin", hBaseAdmin);
    this.giraffaServer
        .setAttribute("masterEnvironment", ctx.getEnvironment());
    this.giraffaServer.start();

  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    shutdown();
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    shutdown();
  }

  private void shutdown() throws IOException {
    LOG.info("Shutting down Giraffa Web Server");
    if (this.giraffaServer != null && this.giraffaServer.isAlive()) {
      try {
        this.giraffaServer.stop();
      } catch (Exception e) {
        LOG.error("Error stopping Giraffa Web Server", e);
      } finally {
        this.giraffaServer = null;
      }
    }
  }

}
