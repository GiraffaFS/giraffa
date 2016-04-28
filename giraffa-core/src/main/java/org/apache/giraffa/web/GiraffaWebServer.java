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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;

import javax.servlet.http.HttpServlet;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

class GiraffaWebServer {

  private final HttpServer2 httpServer;

  GiraffaWebServer(String giraffa,
                   InetSocketAddress infoSocAddr,
                    Configuration conf)
          throws IOException {
    URI uri = URI.create("http://" + NetUtils.getHostPortString(infoSocAddr));
    httpServer = new HttpServer2.Builder().setName(giraffa)
      .hostName(infoSocAddr.getHostName()).setConf(conf)
      .addEndpoint(uri).build();
  }

  void addServlet(String name,
                  String pathSpec,
                  Class<? extends HttpServlet> clazz) {
    httpServer.addServlet(name, pathSpec, clazz);
  }

  void setAttribute(String name,
                    Object value) {
    httpServer.setAttribute(name, value);
  }

  void start() throws IOException {
    httpServer.start();
  }

  boolean isAlive() {
    return httpServer.isAlive();
  }

  void stop() throws Exception {
    httpServer.stop();
  }
}
