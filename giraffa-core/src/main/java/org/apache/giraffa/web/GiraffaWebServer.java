package org.apache.giraffa.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.resource.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;

public class GiraffaWebServer extends HttpServer {

  public GiraffaWebServer(String giraffa, InetSocketAddress infoSocAddr,
                          Configuration conf) throws IOException {
    super(giraffa, infoSocAddr.getHostName(), infoSocAddr.getPort(), false,
        conf);
  }

  /**
   * Get the pathname to the webapps files.
   *
   * @param appName eg "secondary" or "datanode"
   * @return the pathname as a URL
   * @throws java.io.FileNotFoundException if 'webapps' directory cannot be
   * found on CLASSPATH.
   */
  protected String getWebAppsPath(String appName) throws FileNotFoundException {
    // Copied from the super-class.
    // Copied from the super-class.
    String resourceName = "hbase-webapps/" + appName;
    URL url = getClass().getClassLoader().getResource(resourceName);
    if (url == null)
      throw new FileNotFoundException(appName + " not found in CLASSPATH");
    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  @Override
  protected void addDefaultApps(ContextHandlerCollection parent, String appDir,
                                Configuration conf) throws IOException {
    // set up the context for "/static/*"
    Context staticContext = new Context(parent, "/static");
    staticContext
        .setBaseResource(Resource.newResource(appDir + "/giraffa/static"));
    staticContext.addServlet(ContentServlet.class, "/*");
    staticContext.setDisplayName("static");
    defaultContexts.put(staticContext, true);
  }
}
