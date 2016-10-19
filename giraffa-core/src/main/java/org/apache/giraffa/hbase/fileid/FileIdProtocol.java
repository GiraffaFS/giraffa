/*
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
package org.apache.giraffa.hbase.fileid;

import java.io.IOException;

/**
 * A protocol for retrieving the unique id used to represent each file or
 * directory.
 */
interface FileIdProtocol {

  /**
   * Retrieve the id of the file/directory at the given path.
   * @param parentKey bytes of the RowKey of the parent directory
   * @param src the path to the file/directory whose id is needed
   * @return the fixed, unique long representing this file/directory
   */
  long getFileId(byte[] parentKey,
                 String src)
      throws IOException;
}
