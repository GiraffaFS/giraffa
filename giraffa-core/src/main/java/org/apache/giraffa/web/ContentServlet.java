package org.apache.giraffa.web;

import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.resource.Resource;

public class ContentServlet extends DefaultServlet {
  private static final long serialVersionUID = -315035973730134486L;

  @Override
  public Resource getResource(String pathInContext) {
    Resource res = super.getResource(pathInContext);
    if (res != null && res.exists()) {
      return res;
    } else {
      return Resource
          .newClassPathResource("META-INF/resources" + pathInContext);
    }
  }
}
