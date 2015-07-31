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
import org.apache.hadoop.giraffa.protocol.proto.GBMClientProtocolProtos.CloseBlockFileResponseProto;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

public class GBMClientProtocolServerSideTranslatorPB implements
        GBMClientProtocolPB {

  private final GBMClientProtocol impl;

  public GBMClientProtocolServerSideTranslatorPB(
          GBMClientProtocol impl) {
    this.impl = impl;
  }

  @Override
  public AllocateBlockFileResponseProto allocateBlockFile(
          RpcController controller, AllocateBlockFileRequestProto request)
          throws ServiceException {
    List<UnlocatedBlock> blocks
            = new ArrayList<>(request.getBlocksList().size());
    for (UnlocatedBlockProto ub : request.getBlocksList()) {
      ExtendedBlock extBlock = new ExtendedBlock(
              ub.getB().getPoolId(),
              ub.getB().getBlockId());
      extBlock.setGenerationStamp(ub.getB().getGenerationStamp());
      blocks.add(new UnlocatedBlock(extBlock, ub.getOffset(), ub.getCorrupt()));
    }
    try {
      LocatedBlock block = impl.allocateBlockFile(request.getSrc(), blocks,
              request.getClientName());
      return AllocateBlockFileResponseProto.newBuilder().setBlock(PBHelper.
              convert(block)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CloseBlockFileResponseProto closeBlockFile(RpcController controller,
          CloseBlockFileRequestProto request) throws ServiceException {
    try {
      impl.closeBlockFile(PBHelper.convert(request.getBlock()), request.
              getClientName());
      return CloseBlockFileResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

}
