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
  private static final int offset = length - 8;
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

  private String src;
  private byte[] bytes;

  public FileIdRowKey() {}

  public static void setFileIdProtocol(FileIdProtocol protocol) {
    service.set(protocol);
  }

  @Override // RowKey
  public String getPath() {
    return src;
  }

  @Override // RowKey
  public void setPath(String src)
      throws IOException {
    this.src = src;
  }

  @Override // RowKey
  public void set(String src, byte[] bytes)
      throws IOException {
    this.src = src;
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
    Path path = new Path(src);
    if (path.isRoot()) {
      return ROOT_KEY;
    }

    try {
      // get parent key
      RowKey parentKey = RowKeyFactory.newInstance(path.getParent());
      byte[] b = parentKey.getKey();
      assert b.length == length;

      // get file id and append to parent key
      FileIdProtocol protocol = service.get();
      if (protocol == null) {
        throw new RuntimeException("FileIdProtocol implementation not set.");
      }
      long fileId = protocol.getFileId(b, src);
      lshift(b, 8);
      putLong(b, offset, fileId);
      return b;
    } catch (IOException e) {
      LOG.error("Failed to generate row key for " + getPath(), e);
      return ERROR_KEY;
    }
  }

  @Override // RowKey
  public byte[] getStartListingKey(byte[] startAfter) {
    byte[] b;
    if (startAfter.length == 0) {
      b = getKey();
      lshift(b, 8);
      putLong(b, offset, 0);
    } else {
      Path p = new Path(src, new String(startAfter));
      try {
        b = RowKeyFactory.newInstance(p).getKey();
      } catch (IOException e) {
        LOG.error("Failed to get start listing key for " + getPath(), e);
        return ERROR_KEY;
      }
      long id = RowKeyBytes.toLong(b, offset);
      putLong(b, offset, id + 1);
    }
    return b;
  }

  @Override // RowKey
  public byte[] getStopListingKey() {
    byte[] b = getKey();
    lshift(b, 8);
    putLong(b, offset, Long.MAX_VALUE);
    return b;
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
}
