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

import static org.apache.hadoop.hdfs.server.namenode.INodeId.ROOT_INODE_ID;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class INodeIdGenerator {

  private static final String ID_PATH = "/inodeId";
  private static final String LOCK_PATH = "/inodeIdLock";
  private static final RetryPolicy RETRY = new RetryNTimes(100, 10);
  private static final PromotedToLock LOCK = PromotedToLock.builder()
      .lockPath(LOCK_PATH)
      .retryPolicy(RETRY)
      .timeout(100, TimeUnit.MILLISECONDS)
      .build();

  private final CuratorFramework client;
  private final DistributedAtomicLong id;

  INodeIdGenerator(Configuration conf) throws IOException {
    String quorum = ZKConfig.getZKQuorumServersString(conf);
    client = CuratorFrameworkFactory.newClient(quorum, RETRY);
    client.start();
    id = new DistributedAtomicLong(client, ID_PATH, RETRY, LOCK);
    initialize();
  }

  void close() {
    client.close();
  }

  long nextId() throws IOException {
    try {
      return id.increment().postValue();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void initialize() throws IOException {
    try {
      id.initialize(ROOT_INODE_ID);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
