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

import java.io.Serializable;

import org.apache.hadoop.fs.Path;

/**
 * Implementation of a row key based on the file's full path.
 * The key of the row corresponding to a file is the file's full path in the
 * file system directory tree.
 */
class FullPathRowKey extends RowKey implements Serializable {
  private static final long serialVersionUID = 123456789009L;

  private String path;
  private byte[] key;

  public FullPathRowKey() {}

  FullPathRowKey(Path src) {
    initialize(src);
  }

  private void initialize(Path src) {
    // Strip off all URI components: should be pure file path
    this.path = src.toUri().getPath();
    this.key = src.toString().getBytes();
  }

  @Override // RowKey
  public void setPath(Path src) {
    initialize(src);
  }

  @Override
  public Path getPath() {
    return new Path(path);
  }

  @Override // RowKey
  public byte[] getKey() {
    return key;
  }

  @Override // RowKey
  public byte[] generateKey() {
    // the key should already be generated if the path is set
    return key;
  }
}
