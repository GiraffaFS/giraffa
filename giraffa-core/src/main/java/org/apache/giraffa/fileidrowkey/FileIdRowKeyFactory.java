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

import static org.apache.giraffa.GiraffaProtos.FileIdService.newReflectiveService;

import org.apache.giraffa.GiraffaProtos.FileIdService.Interface;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.RowKeyFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class FileIdRowKeyFactory extends RowKeyFactory {

  private FileIdAgent agent;

  @Override // RowKeyFactory
  public void initialize(Table nsTable) {
    agent = new FileIdAgent(nsTable);
    FileIdProcessor processor = new FileIdProcessor(this, nsTable);
    Interface impl = new FileIdProtocolServerSideTranslatorPB(processor);
    setService(newReflectiveService(impl));
  }

  @Override // RowKeyFactory
  protected RowKey getRowKey(String src,
                             long inodeId)
      throws IOException {
    return null; // TODO
  }

  @Override // RowKeyFactory
  protected RowKey getRowKey(String src,
                             byte[] bytes)
      throws IOException {
    return null; // TODO
  }
}
