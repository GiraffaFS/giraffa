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
package org.apache.giraffa.web;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.GiraffaConfiguration;
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

  private Connection connection;

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
    this.connection = ConnectionFactory.createConnection(conf);
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
    if(this.connection != null) {
      connection.close();
    }
  }

}
