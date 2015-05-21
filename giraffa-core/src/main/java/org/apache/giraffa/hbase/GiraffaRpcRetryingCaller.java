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

import java.io.IOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.RetryingCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;

/**
 * This class overrides the callWithRetries method to ensure that FileSystem
 * operations resulting in IOExceptions are not retried and that the raw
 * exceptions are passed to the RPC.
 */
public class GiraffaRpcRetryingCaller<T> extends RpcRetryingCaller<T> {
  public GiraffaRpcRetryingCaller(long pause, int retries,
      int startLogErrorsCnt) {
    super(pause, retries, startLogErrorsCnt);
  }

  @Override
  public synchronized T callWithRetries(RetryingCallable<T> callable, int callTimeout)
      throws IOException, RuntimeException {
    try {
      return this.callWithoutRetries(callable, callTimeout);
    }catch(IOException e) {
      if(e instanceof NotServingRegionException ||
         e instanceof ConnectionClosingException) {
        callable.prepare(true); // reload regions to avoid error
        return this.callWithoutRetries(callable, callTimeout);
      }else {
        throw e;
      }
    }
  }
}
