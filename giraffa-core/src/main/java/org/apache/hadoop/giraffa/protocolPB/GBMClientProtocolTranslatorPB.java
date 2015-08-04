/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.giraffa.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraffa.GiraffaProtos.UnlocatedBlockProto;
import org.apache.giraffa.UnlocatedBlock;
import org.apache.hadoop.giraffa.protocol.GBMClientProtocol;
import org.apache.hadoop.giraffa.protocol.proto.GBMClientProtocolProtos.AllocateBlockFileRequestProto;
import org.apache.hadoop.giraffa.protocol.proto.GBMClientProtocolProtos.AllocateBlockFileResponseProto;
import org.apache.hadoop.giraffa.protocol.proto.GBMClientProtocolProtos.CloseBlockFileRequestProto;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.ipc.ProtobufHelper;

public class GBMClientProtocolTranslatorPB implements
        GBMClientProtocol {

  private final static RpcController NULL_CONTROLLER = null;

  private final GBMClientProtocolPB rpcProxy;

  public GBMClientProtocolTranslatorPB(GBMClientProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public LocatedBlock allocateBlockFile(String src, List<UnlocatedBlock> blocks,
          String clientName) throws IOException {
    List<UnlocatedBlockProto> protoBlocks = new ArrayList<>(blocks.size());
    for (UnlocatedBlock ub : blocks) {
      protoBlocks.add(UnlocatedBlockProto.newBuilder()
              .setB(PBHelper.convert(ub.getBlock()))
              .setOffset(ub.getStartOffset())
              .setCorrupt(ub.isCorrupt())
              .setBlockToken(PBHelper.convert(ub.getBlockToken()))
              .build());
    }
    AllocateBlockFileRequestProto request = AllocateBlockFileRequestProto.
            newBuilder().setSrc(src).addAllBlocks(protoBlocks).setClientName(
                    clientName).build();
    try {
      AllocateBlockFileResponseProto response = rpcProxy.allocateBlockFile(
              NULL_CONTROLLER, request);
      return PBHelper.convert(response.getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void closeBlockFile(ExtendedBlock block, String clientName) throws
          IOException {
    CloseBlockFileRequestProto request = CloseBlockFileRequestProto.newBuilder()
            .setBlock(PBHelper.convert(block))
            .setClientName(clientName)
            .build();
    try {
      rpcProxy.closeBlockFile(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

}
