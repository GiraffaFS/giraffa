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
package org.apache.giraffa.hbase.inodeid;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import org.apache.giraffa.GiraffaProtos.GetINodeIdRequestProto;
import org.apache.giraffa.GiraffaProtos.GetINodeIdResponseProto;
import org.apache.giraffa.GiraffaProtos.INodeIdService.Interface;
import org.apache.giraffa.INodeIdProtocol;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import java.io.IOException;

class INodeIdProtocolServerSideTranslatorPB implements Interface {

  private final INodeIdProtocol server;

  INodeIdProtocolServerSideTranslatorPB(INodeIdProtocol server) {
    this.server = server;
  }

  @Override // Interface
  public void getINodeId(RpcController controller,
                         GetINodeIdRequestProto req,
                         RpcCallback<GetINodeIdResponseProto> done) {
    long inodeId = 0;
    try {
      byte[] parentKey = req.getParentKey().toByteArray();
      String src = req.getSrc();
      inodeId = server.getINodeId(parentKey, src);
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    GetINodeIdResponseProto resp = GetINodeIdResponseProto.newBuilder()
        .setINodeId(inodeId)
        .build();
    done.run(resp);
  }
}
