package org.apache.giraffa;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileIdRowKeyFactory extends RowKeyFactory {

  @Override // RowKeyFactory
  public FileIdRowKey newRowKey(String src) throws IOException {
    return newRowKey(src, -1);
  }

  @Override // RowKeyFactory
  public FileIdRowKey newRowKey(String src, long inodeId) throws IOException {
    Path path = new Path(src);
    if (path.isRoot()) {
      return FileIdRowKey.ROOT;
    }
    FileIdRowKey parent = newRowKey(path.getParent().toString());
    if (inodeId < 0) {
      inodeId = getINodeId(path);
      if (inodeId < 0) {
        return newRowKey(src, FileIdRowKey.EMPTY);
      }
    }
    return new FileIdRowKey(src, parent, inodeId);
  }

  @Override // RowKeyFactory
  public FileIdRowKey newRowKey(String src, byte[] bytes) {
    return new FileIdRowKey(src, bytes);
  }

  private long getINodeId(Path path) throws IOException {
    FileIdRowKey parent = newRowKey(path.getParent().toString());
    return getService().getFileId(parent.getKey(), path.toString());
  }
}
