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

import static org.apache.hadoop.hdfs.server.namenode.INodeId.GRANDFATHER_INODE_ID;

import com.google.protobuf.Service;

import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract {@link RowKey} factory.
 * <p/>
 * The factory is configurable and is instantiated via
 * {@link RowKeyFactoryProvider}.
 * <br/>
 * Subclasses should override getRowKey() methods.
 * To create a new key newInstance() methods should be used.
 * <br/>
 * If {@link Configuration} specifies caching, the keys will be cached 
 * for faster instantiation.<br>
 * This class is thread safe.
 */
public abstract class RowKeyFactory {
  private static Map<String, RowKey> cache;

  private Service service;

  public static synchronized boolean isCaching() {
    return cache != null;
  }

  public static void setCache(boolean caching) {
    synchronized(RowKeyFactory.class) {
      if(caching & cache == null)
        cache = new HashMap<String, RowKey>();
    }
  }

  protected void initialize(Table nsTable) {
    // do nothing
  }

  public Service getService() {
    return service;
  }

  public boolean hasService() {
    return service != null;
  }

  protected void setService(Service service) {
    this.service = service;
  }

  /**
   * Create new instance of RowKey based on file path and inodeId.
   * RowKey.bytes field may remain uninitialized depending on the 
   * file path resolution implementation. {@link RowKey#getKey()} will further
   * generate the bytes.
   * 
   * @param src file path
   * @param inodeId
   * @return new RowKey instance
   * @throws IOException
   */
  public RowKey newInstance(String src, long inodeId) throws IOException {
    return newInstance(src, inodeId, null);
  }

  public RowKey newInstance(String src) throws IOException {
    return newInstance(src, GRANDFATHER_INODE_ID, null);
  }

  public RowKey newInstance(String src, byte[] bytes) throws IOException {
    return newInstance(src, GRANDFATHER_INODE_ID, bytes);
  }

  /**
   * Create new instance of RowKey based on file path, inodeId, and key bytes.
   * If bytes is not null, then RowKey will be fully defined in this case,
   * and no file path resolution to generate bytes is necessary.
   * This can be used when the key is returned from the namespace service
   * as a byte array.
   * 
   * @param src file path
   * @return new RowKey instance
   * @throws IOException
   */
  public RowKey newInstance(String src, long inodeId, byte[] bytes)
      throws IOException {
    // try cache
    RowKey key = null;
    synchronized(RowKeyFactory.class) {
      key = isCaching() ? cache.get(src) : null;
    }
    if(key != null)
      return key;

    // generate new RowKey
    return createRowKey(src, inodeId, bytes);
  }

  private RowKey createRowKey(String src, long inodeId, byte[] bytes)
      throws IOException {
    RowKey key = (bytes == null) ? getRowKey(src, inodeId) :
                                   getRowKey(src, bytes);

    synchronized(RowKeyFactory.class) {
      if(isCaching())
        cache.put(src, key);
    }
    return key;
  }

  protected abstract RowKey getRowKey(String src, long inodeId)
      throws IOException;

  protected abstract RowKey getRowKey(String src, byte[] bytes)
      throws IOException;
}
