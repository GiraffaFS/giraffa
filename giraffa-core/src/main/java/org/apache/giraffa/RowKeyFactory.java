package org.apache.giraffa;

import java.io.IOException;

public abstract class RowKeyFactory {

  private GiraffaProtocol service;

  void setService(GiraffaProtocol service) {
    this.service = service;
  }

  public GiraffaProtocol getService() {
    return service;
  }

  public abstract RowKey newRowKey(String src) throws IOException;

  public abstract RowKey newRowKey(String src, long inodeId) throws IOException;

  public abstract RowKey newRowKey(String src, byte[] bytes) throws IOException;
}
