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

/**
 * Abstract class defining a row key for a file system object in the
 * Namespace Table in HBase.<br>
 * The key is an arbitrary byte array, accessible via {@link #getKey()}.<br>
 * A row key implementation defines a particular ordering of the objects in the
 * Namespace Table.
 * Based on that ordering HBase partitions tables into regions.
 * Hence, the row key ordering defines the locality of the file system
 * objects, because adjacent objects are likely to fall into the same region.
 * <p>
 * Extend this class to define a specific row key implementation.
 */
public abstract class RowKey {

  /**
   * Get full path of the file system object represented by the underlying row.
   * @return full path
   */
  public abstract String getPath();

  /**
   * Get the row key of the file system object.
   * The method should generate the key if it has not been generated before
   * or return the generated value.
   * @return row key as a byte array
   */
  public abstract byte[] getKey();

  /**
   * Generate or regenerate the row key based on the path.
   * Key generation can be a distributed operation for some RowKey
   * implementations.
   * @return row key as a byte array
   */
  public abstract byte[] generateKey();

  public abstract byte[] getStartListingKey(byte[] startAfter);

  public abstract byte[] getStopListingKey();

  @Override // Object
  public abstract boolean equals(Object o);

  @Override // Object
  public abstract int hashCode();

  @Override // Object
  public String toString() {
    return getClass().getSimpleName() + ": " + getKeyString()
        +" | " + getPath();
  }

  /**
   * Return a String representation of the underlying byte array for use in
   * {@link #toString()}.
   */
  public abstract String getKeyString();
}
