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
package org.apache.giraffa.hbase.fileid;

import static org.apache.giraffa.GiraffaProtos.FileIdService.newReflectiveService;
import static org.apache.giraffa.RowKeyFactoryProvider.createFactory;

import com.google.protobuf.Service;

import org.apache.giraffa.FileIdProtocol;
import org.apache.giraffa.GiraffaConfiguration;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.RowKeyFactory;
import org.apache.giraffa.hbase.INodeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

import java.io.IOException;

public class FileIdProcessor implements
    Coprocessor, CoprocessorService, FileIdProtocol {

  private final Service service;

  private RowKeyFactory keyFactory;
  private INodeManager nodeManager;

  public FileIdProcessor() {
    service = newReflectiveService(
        new FileIdProtocolServerSideTranslatorPB(this));
  }

  @Override // Coprocessor
  public void start(CoprocessorEnvironment env)
      throws IOException {
    Configuration conf = env.getConfiguration();
    String tableName = GiraffaConfiguration.getGiraffaTableName(conf);
    Table nsTable = env.getTable(TableName.valueOf(tableName));
    keyFactory = createFactory(conf);
    nodeManager = new INodeManager(keyFactory, nsTable);
  }

  @Override // Coprocessor
  public void stop(CoprocessorEnvironment env) {
    nodeManager.close();
  }

  @Override // CoprocessorService
  public Service getService() {
    return service;
  }

  @Override // FileIdProtocol
  public long getFileId(byte[] parentKey,
                        String src)
      throws IOException {
    String parent = new Path(src).getParent().toString();
    RowKey parentRowKey = keyFactory.newInstance(parent, parentKey);
    return nodeManager.getINode(parentRowKey, src).getId();
  }
}
