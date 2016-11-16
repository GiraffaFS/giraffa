package org.apache.giraffa;

import org.apache.giraffa.hbase.fileid.FileIdAgent;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.giraffa.RowKeyBytes.lshift;
import static org.apache.giraffa.RowKeyBytes.putLong;
import static org.apache.hadoop.hdfs.server.namenode.INodeId.GRANDFATHER_INODE_ID;
import static org.apache.hadoop.hdfs.server.namenode.INodeId.ROOT_INODE_ID;

class FileIdRowKey extends RowKey {

  private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  private static final byte[] EMPTY = new byte[0];

  private final Path path;
  private final int depth;
  private final RowKeyFactory keyFactory;
  private final FileIdAgent agent;

  private long inodeId;
  private byte[] parentKey;
  private byte[] bytes;

  FileIdRowKey(Path path,
               long inodeId,
               byte[] bytes,
               int depth,
               RowKeyFactory keyFactory,
               FileIdAgent agent) {
    this.path = path;
    this.inodeId = inodeId;
    this.bytes = bytes;
    this.depth = depth;
    this.keyFactory = keyFactory;
    this.agent = agent;
  }

  @Override
  public String getPath() {
    return path.toString();
  }

  private byte[] getKeyOrEmptyBytes() {
    try {
      return getKey();
    } catch (IOException ignored) {
      return EMPTY;
    }
  }

  private long getINodeId()
      throws IOException {
    if (inodeId == GRANDFATHER_INODE_ID) {
      inodeId = lookupINodeId();
    }
    return inodeId;
  }

  private long lookupINodeId()
      throws IOException {
    if (path.isRoot()) {
      return ROOT_INODE_ID;
    } else {
      return agent.getFileId(getParentKey(), path.toString());
    }
  }

  private byte[] getParentKey()
      throws IOException {
    if (parentKey == null) {
      parentKey = lookupParentKey();
    }
    return parentKey.clone();
  }

  private byte[] lookupParentKey()
      throws IOException {
    if (path.isRoot()) {
      return new byte[depth * LONG_BYTES];
    } else {
      String parent = path.getParent().toString();
      return keyFactory.newInstance(parent).getKey();
    }
  }

  @Override
  public byte[] getKey()
      throws IOException {
    if(bytes == null) {
      bytes = generateKey();
    }
    return bytes.clone();
  }

  @Override
  public byte[] generateKey()
      throws IOException {
    return toChildKey(getParentKey(), getINodeId());
  }

  @Override
  public byte[] getStartListingKey(byte[] startAfter)
      throws IOException {
    return toChildKey(getKey(), 0);
  }

  @Override
  public byte[] getStopListingKey()
      throws IOException {
    return toChildKey(getKey(), Long.MAX_VALUE);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FileIdRowKey)) return false;

    FileIdRowKey that = (FileIdRowKey) o;
    byte[] key = getKeyOrEmptyBytes();
    return key.length > 0 && Arrays.equals(key, that.getKeyOrEmptyBytes());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(getKeyOrEmptyBytes());
  }

  @Override
  public String getKeyString() {
    return RowKeyBytes.toString(getKeyOrEmptyBytes());
  }

  private byte[] toChildKey(byte[] parentKey, long childId) {
    int shiftLength = parentKey.length - LONG_BYTES;
    lshift(parentKey, LONG_BYTES);
    putLong(parentKey, shiftLength + 1, childId);
    return parentKey;
  }
}
