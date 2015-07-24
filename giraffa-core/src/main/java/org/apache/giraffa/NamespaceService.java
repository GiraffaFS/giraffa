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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/**
 * {@link NamespaceService} is a common interface that provides access
 * to a reliable storage system, like HBase, which maintains 
 * the Giraffa file system metadata.
 * <p>
 * It plays a role of a proxy used by DFSClient to communicate with
 * the underlying storage system as if it is a NameNode.
 * It implements {@link ClientProtocol} and is a replacement of the
 * NameNode RPC proxy.
 * <p>
 * Implement this interface to make Giraffa client connect to different
 * highly available storage systems.
 * <p>
 * {@link org.apache.giraffa.hbase.NamespaceAgent} is the default implementation of
 * {@link NamespaceService} for HBase.
 */
public interface NamespaceService
    extends ClientProtocol, FileIdProtocol, Closeable {
  
  public void initialize(GiraffaConfiguration conf) throws IOException;

  public void format(GiraffaConfiguration conf) throws IOException;
}
