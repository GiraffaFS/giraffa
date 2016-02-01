/**
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
package org.apache.giraffa.fileidrowkey;

import org.apache.giraffa.RowKeyFactory;
import org.apache.giraffa.hbase.INodeManager;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class FileIdProcessor implements FileIdProtocol {

  private final INodeManager manager;

  public FileIdProcessor(RowKeyFactory keyFactory,
                         Table nsTable) {
    manager = new INodeManager(keyFactory, nsTable);
  }

  @Override // FileIdProtocol
  public long getFileId(byte[] parentKey,
                        String src)
      throws IOException {
    return 0; // TODO
  }
}
