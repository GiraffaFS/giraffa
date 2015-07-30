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
package org.apache.giraffa.hbase;

import static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName;

import com.google.protobuf.Service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FileIdProtocol;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.RowKeyFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;

public class FileIdProcessor
    implements Coprocessor, CoprocessorService, FileIdProtocol {

  private static final Log LOG = LogFactory.getLog(FileIdProcessor.class);

  private final Service service;
  private INodeManager nodeManager;

  public FileIdProcessor() {
    service = new FileIdProtocolServiceServerSideTranslatorPB(this);
  }

  @Override // Coprocessor
  public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof RegionCoprocessorEnvironment)) {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
    LOG.info("Start FileIdProcessor...");
    Configuration conf = env.getConfiguration();
    TableName tableName = TableName.valueOf(getGiraffaTableName(conf));
    nodeManager = new INodeManager(env.getTable(tableName));
  }

  @Override // Coprocessor
  public void stop(CoprocessorEnvironment env) throws IOException {
    LOG.info("Stopping FileIdProcessor...");
    if (nodeManager != null) {
      nodeManager.close();
    }
  }

  @Override // CoprocessorService
  public Service getService() {
    return service;
  }

  @Override // FileIdProtocol
  public long getFileId(byte[] parentKey, String src) throws IOException {
    Path srcPath = new Path(src);
    String parentPath = srcPath.getParent().toString();
    RowKey parentRowKey = RowKeyFactory.newInstance(parentPath, -1, parentKey);
    return nodeManager.findINodeId(parentRowKey, srcPath);
  }
}
