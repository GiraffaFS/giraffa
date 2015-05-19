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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;

/**
 * Hook for loading our own RpcRetryingCaller to block filesystem operations
 * from being retried and properly handle exceptions. The name of this class
 * is specified in the property "hbase.rpc.callerfactory.class"
 */
public class GiraffaRpcRetryingCallerFactory extends RpcRetryingCallerFactory {

  public static final int START_LOG_ERRORS_CNT = 9;

  public GiraffaRpcRetryingCallerFactory(Configuration conf) {
    super(conf);
  }

  @Override
  public <T> RpcRetryingCaller<T> newCaller() {
    long pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    int retries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    int startLogErrorsCnt = START_LOG_ERRORS_CNT;
    return new GiraffaRpcRetryingCaller<T>(pause, retries, startLogErrorsCnt);
  }
}
