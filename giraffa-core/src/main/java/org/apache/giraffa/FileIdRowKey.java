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
package org.apache.giraffa;

import static org.apache.giraffa.RowKeyBytes.lshift;
import static org.apache.giraffa.RowKeyBytes.putLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;

public class FileIdRowKey extends RowKey implements Serializable {

  private static final long serialVersionUID = 123456789009L;
  private static final Log LOG = LogFactory.getLog(FileIdRowKey.class);

  private static final int depth = 3;
  private static final int length = 8 * depth;
  private static final int lastIdOffset = length - 8;
  private static final byte[] ROOT_KEY = new byte[length];
  private static final byte[] ERROR_KEY = new byte[length];
  private static final ThreadLocal<FileIdProtocol> service =
      new ThreadLocal<>();

  static {
    putLong(ROOT_KEY, length - 8, 1000L);
    for (int i = 0; i < length; i += 8) {
      putLong(ERROR_KEY, i, Long.MAX_VALUE);
    }
  }

  public static void setFileIdProtocol(FileIdProtocol protocol) {
    service.set(protocol);
  }

  private String src;
  private long inodeId = -1;
  private byte[] bytes;
  private byte[] parentKey;
  private boolean shouldCache = true;

  public FileIdRowKey() {}

  @Override // RowKey
  public String getPath() {
    return src;
  }

  @Override // RowKey
  public void setPath(String src) throws IOException {
    this.src = src;
  }

  @Override // RowKey
  public long getINodeId() throws IOException {
    if (inodeId == -1) {
      if (bytes != null) {
        inodeId = RowKeyBytes.toLong(bytes, length - 8);
      } else {
        FileIdProtocol protocol = service.get();
        assert protocol != null;
        inodeId = protocol.getFileId(getParentKey(), getPath());
      }
    }
    assert inodeId >= 0;
    return inodeId;
  }

  @Override // RowKey
  public void setINodeId(long inodeId) {
    this.inodeId = inodeId;
  }

  @Override // RowKey
  public void set(String src, long inodeId, byte[] bytes) throws IOException {
    setPath(src);
    setINodeId(inodeId);
    this.bytes = bytes;
  }

  @Override // RowKey
  public byte[] getKey() {
    if (bytes == null) {
      bytes = generateKey();
    }
    return bytes.clone();
  }

  @Override // RowKey
  public byte[] generateKey() {
    if (new Path(getPath()).isRoot()) {
      return ROOT_KEY;
    }

    try {
      byte[] b = getParentKey();
      long fileId = getINodeId();
      shouldCache &= fileId != 0;
      lshift(b, 8);
      putLong(b, lastIdOffset, fileId);
      return b;
    } catch (IOException e) {
      LOG.error("Failed to generate row key for " + getPath(), e);
      shouldCache = false;
      return ERROR_KEY;
    }
  }

  @Override // RowKey
  public byte[] getStartListingKey(byte[] startAfter) {
    byte[] b;
    if (startAfter.length == 0) {
      b = getKey();
      lshift(b, 8);
      putLong(b, lastIdOffset, 0);
    } else {
      try {
        Path p = new Path(getPath(), new String(startAfter));
        RowKey startKey = RowKeyFactory.newInstance(p.toString());
        b = startKey.getKey();
        putLong(b, lastIdOffset, startKey.getINodeId() + 1);
      } catch (IOException e) {
        LOG.error("Failed to get start listing key for " + getPath(), e);
        return ERROR_KEY;
      }
    }
    return b;
  }

  @Override // RowKey
  public byte[] getStopListingKey() {
    byte[] b = getKey();
    lshift(b, 8);
    putLong(b, lastIdOffset, Long.MAX_VALUE);
    return b;
  }

  @Override // RowKey
  public boolean shouldCache() {
    if (bytes == null) {
      bytes = generateKey();
    }
    return shouldCache;
  }

  @Override // RowKey
  public String getKeyString() {
    byte[] b = getKey();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i += 8) {
      if (i > 0) sb.append('/');
      sb.append(RowKeyBytes.toLong(b, i));
    }
    return sb.toString();
  }

  private byte[] getParentKey() throws IOException {
    if (parentKey == null) {
      String parent = new Path(getPath()).getParent().toString();
      parentKey = RowKeyFactory.newInstance(parent).getKey();
    }
    return parentKey;
  }
}
