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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.RetryNTimes;
import org.apache.giraffa.id.DistributedId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;

public class ZookeeperId implements DistributedId {

  private static final long INITIAL = -1;
  private static final RetryPolicy RETRY = new RetryNTimes(100, 10);
  private static final long TIMEOUT = 100;

  private final String name;
  private final CuratorFramework client;
  private final DistributedAtomicLong id;

  public ZookeeperId(String name, Configuration conf) {
    String idPath = "/ids" + name + "/id";
    String lockPath = "/ids" + name + "/lock";
    PromotedToLock lock = PromotedToLock.builder()
        .lockPath(lockPath)
        .retryPolicy(RETRY)
        .timeout(TIMEOUT, MILLISECONDS)
        .build();
    String quorum = ZKConfig.getZKQuorumServersString(conf);
    this.name = name;
    this.client = CuratorFrameworkFactory.newClient(quorum, RETRY);
    this.id = new DistributedAtomicLong(client, idPath, RETRY, lock);
  }

  @Override // IdGenerator
  public long nextValue() {
    AtomicValue<Long> value;
    try {
      value = id.increment();
    } catch (Exception e) {
      throw new RuntimeException("Failed to increment: " + name, e);
    }
    if (value.succeeded()) {
      return value.postValue();
    } else {
      throw new RuntimeException("Failed to increment: " + name);
    }
  }

  @Override // DistributedId
  public long initialValue() {
    return INITIAL;
  }

  @Override // DistributedId
  public void start() {
    client.start();
    try {
      id.initialize(INITIAL);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start: " + name, e);
    }
  }

  @Override // DistributedId
  public void close() {
    client.close();
  }
}
