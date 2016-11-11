/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraffa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static org.apache.giraffa.GiraffaConfiguration.GRFA_FILEIDROWKEY_DEPTH;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_FILEIDROWKEY_DEPTH_DEFAULT;
import static org.apache.hadoop.hdfs.server.namenode.INodeId.GRANDFATHER_INODE_ID;

public class FileIdRowKeyFactory extends RowKeyFactory {

  private FileIdProtocol idService;
  private int depth;

  @Override // RowKeyFactory
  public void initialize(Configuration conf)
      throws IOException {
    GiraffaConfiguration grfaConf = new GiraffaConfiguration(conf);
    idService = grfaConf.newFileIdService();
    depth = grfaConf.getInt(GRFA_FILEIDROWKEY_DEPTH, GRFA_FILEIDROWKEY_DEPTH_DEFAULT);
  }

  @Override // RowKeyFactory
  protected RowKey getRowKey(String src, long inodeId) throws IOException {
    return getRowKey(src, inodeId, null);
  }

  @Override // RowKeyFactory
  protected RowKey getRowKey(String src, byte[] bytes) throws IOException {
    return getRowKey(src, GRANDFATHER_INODE_ID, bytes);
  }

  private RowKey getRowKey(String src, long inodeId, byte[] bytes) {
    return new FileIdRowKey(new Path(src), inodeId, bytes, depth, this, idService);
  }
}
