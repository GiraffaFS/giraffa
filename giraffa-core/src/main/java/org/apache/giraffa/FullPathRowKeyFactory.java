package org.apache.giraffa;

import java.io.IOException;

public class FullPathRowKeyFactory extends RowKeyFactory {

  @Override // RowKeyFactory
  public RowKey newRowKey(String src) throws IOException {
    return new FullPathRowKey(src);
  }

  @Override // RowKeyFactory
  public RowKey newRowKey(String src, long inodeId) throws IOException {
    return newRowKey(src);
  }

  @Override // RowKeyFactory
  public RowKey newRowKey(String src, byte[] bytes) throws IOException {
    return new FullPathRowKey(src, bytes);
  }
}
