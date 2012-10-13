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

import org.apache.hadoop.fs.Path;
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

  FullPathRowKey(Path src) throws IOException {
    setPath(src);
  }

  private void initialize(short d, String src) {
    // Strip off all URI components: should be pure file path
    this.path = src;
    this.depth = (short) d;
    this.bytes = null;  // not generated yet
  }

  @Override // RowKey
  public void setPath(Path src) throws IOException {
    // Strip off all URI components: should be pure file path
    String s = src.toUri().getPath();
    int d = src.depth();
    assert d < Short.MAX_VALUE : "Path is too deep";
    initialize((short)d, s);
  }

  @Override
  public Path getPath() {
    return new Path(path);
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
    startKey.initialize((short) (depth + 1), startPath);
    return startKey.getKey();
  }
}
