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
package org.apache.giraffa;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.giraffa.GiraffaProtos.GetFileIdRequestProto;
import org.apache.giraffa.GiraffaProtos.GetFileIdResponseProto;
import org.apache.giraffa.GiraffaProtos.GiraffaProtocolService;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;

import java.io.IOException;

public class GiraffaProtocolServiceServerSideTranslatorPB
    extends ClientNamenodeProtocolServerSideTranslatorPB
    implements GiraffaProtocolService.BlockingInterface {

  private final GiraffaProtocol server;

  public GiraffaProtocolServiceServerSideTranslatorPB(GiraffaProtocol server)
      throws IOException {
    super(server);
    this.server = server;
  }

  @Override
  public GetFileIdResponseProto getFileId(RpcController controller,
                                          GetFileIdRequestProto req)
      throws ServiceException {
    try {
      long result =
          server.getFileId(req.getParentKey().toByteArray(), req.getSrc());
      return GetFileIdResponseProto.newBuilder().setFileId(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
