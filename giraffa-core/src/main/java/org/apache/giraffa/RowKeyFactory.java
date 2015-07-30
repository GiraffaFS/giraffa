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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Create new {@link RowKey} using this factory.
 * The factory should be first initialized by calling
 * {@link #registerRowKey(Configuration)}.
 * If {@link Configuration} specifies caching, the keys will be cached 
 * for faster instantiation.<br>
 * This class is thread safe.
 */
public class RowKeyFactory {
  private static Map<String, RowKey> Cache;
  private static Class<? extends RowKey> RowKeyClass;

  public static synchronized boolean isCaching() {
    return Cache != null;
  }

  public static synchronized Class<? extends RowKey> getRowKeyClass() {
    return RowKeyClass;
  }

  /**
   * Register RowKey class, specified by {@link Configuration} and 
   * turn on caching is requested.
   * @param conf configuration specifying row key class and caching choice.
   */
  public static void registerRowKey(Configuration conf) {
    boolean caching = conf.getBoolean(GiraffaConfiguration.GRFA_CACHING_KEY,
        GiraffaConfiguration.GRFA_CACHING_DEFAULT);
    synchronized(RowKeyFactory.class) {
      if(caching)
        Cache = new HashMap<String, RowKey>();
      RowKeyClass = conf.getClass(GiraffaConfiguration.GRFA_ROW_KEY_KEY,
          GiraffaConfiguration.GRFA_ROW_KEY_DEFAULT, RowKey.class);
    }
  }

  /**
   * Create new instance of RowKey based on file path.
   * RowKey.bytes field may remain uninitialized depending on the 
   * file path resolution implementation. {@link RowKey#getKey()} will further
   * generate the bytes.
   * 
   * @param src file path
   * @return new RowKey instance
   * @throws IOException
   */
  public static RowKey newInstance(String src) throws IOException {
    return newInstance(src, -1);
  }

  /**
   * Create new instance of RowKey based on file path and inode id.
   * RowKey.bytes field may remain uninitialized depending on the
   * file path resolution implementation. {@link RowKey#getKey()} will further
   * generate the bytes.
   *
   * @param src file path
   * @param inodeId id of the INode located at {@code src}; 0 if the path is
   *                nonexistent; -1 if unknown.
   * @return new RowKey instance
   * @throws IOException
   */
  public static RowKey newInstance(String src, long inodeId)
      throws IOException {
    return newInstance(src, inodeId, null);
  }

  /**
   * Create new instance of RowKey based on file path, inode id, and key bytes.
   * RowKey will be fully defined in this case.
   * No file path resolution to generate bytes is necessary.
   * This can be used when the key is returned from the namespace service
   * as a byte array.
   * 
   * @param src file path
   * @param inodeId id of the INode located at {@code src}; 0 if the path is
   *                nonexistent; -1 if unknown.
   * @return new RowKey instance
   * @throws IOException
   */
  public static RowKey newInstance(String src, long inodeId, byte[] bytes)
      throws IOException {
    // try cache
    RowKey key = null;
    synchronized(RowKeyFactory.class) {
      key = isCaching() ? Cache.get(src) : null;
    }
    if(key != null)
      return key;

    // generate new RowKey
    return createRowKey(src, inodeId, bytes);
  }

  public static RowKey createRowKey(String src, long inodeId, byte[] bytes)
      throws IOException {
    RowKey key = ReflectionUtils.newInstance(RowKeyClass, null);
    if(bytes == null) {
      key.setPath(src);
      key.setINodeId(inodeId);
    }
    else
      key.set(src, inodeId, bytes);

    synchronized(RowKeyFactory.class) {
      if(isCaching() && key.shouldCache())
        Cache.put(src, key);
    }
    return key;
  }
}
