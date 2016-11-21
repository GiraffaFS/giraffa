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
package org.apache.giraffa.hbase.inodeid;

import static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName;
import static org.apache.giraffa.GiraffaProtos.INodeIdService.newBlockingStub;

import com.google.protobuf.BlockingRpcChannel;

import org.apache.giraffa.INodeIdProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class INodeIdAgent implements INodeIdProtocol {

  private final Table table;

  @SuppressWarnings("unused")
  public INodeIdAgent(Configuration conf)
      throws IOException {
    this(getTable(conf));
  }

  private INodeIdAgent(Table table) {
    this.table = table;
  }

  @Override // INodeIdProtocol
  public long getINodeId(byte[] parentKey,
                         String src)
      throws IOException {
    return getRegionProxy(parentKey).getINodeId(parentKey, src);
  }

  private INodeIdProtocol getRegionProxy(byte[] key) {
    BlockingRpcChannel channel = table.coprocessorService(key);
    return new INodeIdProtocolTranslatorPB(newBlockingStub(channel));
  }

  private static Table getTable(Configuration conf)
      throws IOException {
    Connection connection = ConnectionFactory.createConnection(conf);
    String tableName = getGiraffaTableName(conf);
    return connection.getTable(TableName.valueOf(tableName));
  }
}
