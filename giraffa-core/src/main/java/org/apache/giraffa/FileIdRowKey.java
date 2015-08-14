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

import java.io.Serializable;
import java.util.Arrays;

public class FileIdRowKey extends RowKey implements Serializable {

  private static final long serialVersionUID = 123456789009L;

  private static final int DEPTH = 3;
  private static final int LENGTH = 8 * DEPTH;
  private static final int LAST_ID_OFFSET = LENGTH - 8;

  public static final FileIdRowKey ROOT;
  public static final byte[] EMPTY;

  static {
    byte[] b = new byte[LENGTH];
    putLong(b, LAST_ID_OFFSET, 1000L);
    ROOT = new FileIdRowKey("/", b);
    EMPTY = new byte[LENGTH];
  }

  private final String src;
  private final RowKey parent;
  private final long inodeId;

  private byte[] bytes;

  public FileIdRowKey(String src, RowKey parent, long inodeId) {
    this.src = src;
    this.parent = parent;
    this.inodeId = inodeId;
  }

  public FileIdRowKey(String src, byte[] bytes) {
    this(src, null, 0);
    this.bytes = bytes;
  }

  @Override // RowKey
  public String getPath() {
    return src;
  }

  @Override // RowKey
  public byte[] getKey() {
    generateKeyIfNull();
    return bytes.clone();
  }

  @Override // RowKey
  public byte[] generateKey() {
    byte[] b = parent.getKey();
    lshift(b, 8);
    putLong(b, LAST_ID_OFFSET, inodeId);
    return b;
  }

  @Override // RowKey
  public byte[] getStartListingKey(byte[] startAfter) {
    byte[] b = getKey();
    lshift(b, 8);
    putLong(b, LAST_ID_OFFSET, 0);
    return b;
  }

  @Override // RowKey
  public byte[] getStopListingKey() {
    byte[] b = getKey();
    lshift(b, 8);
    putLong(b, LAST_ID_OFFSET, Long.MAX_VALUE);
    return b;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FileIdRowKey)) return false;

    FileIdRowKey that = (FileIdRowKey) o;
    generateKeyIfNull();
    that.generateKeyIfNull();
    return Arrays.equals(bytes, that.bytes);
  }

  @Override // Object
  public int hashCode() {
    generateKeyIfNull();
    return Arrays.hashCode(bytes);
  }

  @Override // RowKey
  public String getKeyString() {
    generateKeyIfNull();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < LENGTH; i += 8) {
      if (i > 0) sb.append('/');
      sb.append(RowKeyBytes.toLong(bytes, i));
    }
    return sb.toString();
  }

  private void generateKeyIfNull() {
    if (bytes == null) {
      bytes = generateKey();
    }
  }
}
