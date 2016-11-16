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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import org.apache.giraffa.GiraffaProtos.FileIdService.Interface;
import org.apache.giraffa.GiraffaProtos.GetFileIdRequestProto;
import org.apache.giraffa.GiraffaProtos.GetFileIdResponseProto;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import java.io.IOException;

class FileIdProtocolServerSideTranslatorPB implements Interface {

  private final FileIdProtocol server;

  FileIdProtocolServerSideTranslatorPB(FileIdProtocol server) {
    this.server = server;
  }

  @Override // Interface
  public void getFileId(RpcController controller,
                        GetFileIdRequestProto req,
                        RpcCallback<GetFileIdResponseProto> done) {
    long fileId = 0;
    try {
      byte[] parentKey = req.getParentKey().toByteArray();
      String src = req.getSrc();
      fileId = server.getFileId(parentKey, src);
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    GetFileIdResponseProto resp = GetFileIdResponseProto.newBuilder()
        .setFileId(fileId)
        .build();
    done.run(resp);
  }
}
