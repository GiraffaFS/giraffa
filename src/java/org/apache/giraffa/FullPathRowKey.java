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

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementation of a row key based on the file's full path.
 * The key of the row corresponding to a file is the file's full path in the
 * file system directory tree.
 */
public class FullPathRowKey extends RowKey implements Serializable {
  private static final long serialVersionUID = 123456789009L;

  private short depth;
  private String path;
  private byte[] bytes;

  public FullPathRowKey() {}

  FullPathRowKey(String src) throws IOException {
    setPath(src);
  }

  private void initialize(short d, String src, byte[] bytes) {
    // Strip off all URI components: should be pure file path
    this.path = src;
    this.depth = (short) d;
    this.bytes = bytes;  // not generated yet
  }

  @Override // RowKey
  public void setPath(String src) throws IOException {
    if(!src.startsWith(SEPARATOR))
      throw new IOException("Cannot calculate key for a relative path: " + src);
    int d = depth(src);
    assert d < Short.MAX_VALUE : "Path is too deep";
    initialize((short)d, src, null);
  }

  @Override // RowKey
  public void set(String src, byte[] bytes) throws IOException {
    initialize(Bytes.toShort(bytes), src, bytes);
    assert Bytes.compareTo(Bytes.toBytes(src), 0, Bytes.toBytes(src).length,
        bytes, 2, bytes.length-2) == 0 :
          "Path and key don't match path = " + src +
          " key = " + Bytes.toString(bytes, 2, bytes.length-2);
    
  }
  public static final String SEPARATOR = "/";
  /**
   * Return the number of elements in this path.
   */
  public static int depth(String path) {
    int depth = 0;
    int slash =
        path.length()==1 && path.charAt(0)==SEPARATOR.charAt(0) ? -1 : 0;
    while (slash != -1) {
      depth++;
      slash = path.indexOf(SEPARATOR, slash+1);
    }
    return depth;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override // RowKey
  public byte[] getKey() {
    if(bytes == null)
      bytes = generateKey();
    return bytes;
  }

  @Override // RowKey
  public byte[] generateKey() {
    return Bytes.add(Bytes.toBytes(depth), path.getBytes());
  }

  @Override // RowKey
  public byte[] getStartListingKey(byte[] startAfter) {
    byte[] start = directoryStartKey();
    return startAfter.length == 0 ? start : Bytes.add(start, startAfter);
  }

  @Override // RowKey
  public byte[] getStopListingKey() {
    return Bytes.add(directoryStartKey(), new byte[]{Byte.MAX_VALUE});
  }

  private byte[] directoryStartKey() {
    String startPath = path.endsWith("/") ? path : path + "/";
    FullPathRowKey startKey = new FullPathRowKey();
    startKey.initialize((short) (depth + 1), startPath, null);
    return startKey.getKey();
  }
}
