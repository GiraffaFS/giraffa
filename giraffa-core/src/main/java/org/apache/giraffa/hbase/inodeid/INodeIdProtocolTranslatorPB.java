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

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

import org.apache.giraffa.GiraffaProtos.GetINodeIdRequestProto;
import org.apache.giraffa.GiraffaProtos.GetINodeIdResponseProto;
import org.apache.giraffa.GiraffaProtos.INodeIdService.BlockingInterface;
import org.apache.giraffa.INodeIdProtocol;
import org.apache.hadoop.ipc.ProtobufHelper;

import java.io.IOException;

class INodeIdProtocolTranslatorPB implements INodeIdProtocol {

  private final BlockingInterface rpcProxy;

  INodeIdProtocolTranslatorPB(BlockingInterface rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override // INodeIdProtocol
  public long getINodeId(byte[] parentKey,
                         String src)
      throws IOException {
    GetINodeIdRequestProto req = GetINodeIdRequestProto.newBuilder()
        .setParentKey(ByteString.copyFrom(parentKey))
        .setSrc(src)
        .build();
    try {
      GetINodeIdResponseProto resp = rpcProxy.getINodeId(null, req);
      return resp.getINodeId();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
}
