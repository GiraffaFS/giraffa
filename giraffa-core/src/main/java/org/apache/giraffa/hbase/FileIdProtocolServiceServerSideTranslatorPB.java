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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import org.apache.giraffa.FileIdProtocol;
import org.apache.giraffa.GiraffaProtos.FileIdProtocolService;
import org.apache.giraffa.GiraffaProtos.GetFileIdRequestProto;
import org.apache.giraffa.GiraffaProtos.GetFileIdResponseProto;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import java.io.IOException;

public class FileIdProtocolServiceServerSideTranslatorPB
    extends FileIdProtocolService {

  private FileIdProtocol impl;

  public FileIdProtocolServiceServerSideTranslatorPB(FileIdProtocol impl) {
    this.impl = impl;
  }

  @Override // FileIdProtocolService
  public void getFileId(RpcController controller,
                        GetFileIdRequestProto request,
                        RpcCallback<GetFileIdResponseProto> done) {
    try {
      byte[] parentKey = request.getParentKey().toByteArray();
      String src = request.getSrc();
      long fileId = impl.getFileId(parentKey, src);
      done.run(GetFileIdResponseProto.newBuilder().setFileId(fileId).build());
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
      done.run(null);
    }
  }
}
