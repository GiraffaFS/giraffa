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
import org.apache.giraffa.id.IdGeneratorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;

/**
 * Sequential ID generator that uses Zookeeper to store and compute ids.
 * Increments are done atomically, and this class is thread-safe.
 */
public class ZKSequentialNumber implements IdGeneratorService {

  private static final RetryPolicy RETRY = new RetryNTimes(100, 10);
  private static final long TIMEOUT = 100;

  private final String name;
  private final long initialValue;
  private final CuratorFramework client;
  private final DistributedAtomicLong id;

  /**
   * @param name unique name identifying this generator's data on Zookeeper
   * @param conf configuration information for connecting to Zookeeper
   */
  public ZKSequentialNumber(String name,
                            long initialValue,
                            Configuration conf) {
    String idPath = "/ids" + name + "/id";
    String lockPath = "/ids" + name + "/lock";
    PromotedToLock lock = PromotedToLock.builder()
        .lockPath(lockPath)
        .retryPolicy(RETRY)
        .timeout(TIMEOUT, MILLISECONDS)
        .build();
    String quorum = ZKConfig.getZKQuorumServersString(conf);
    this.name = name;
    this.initialValue = initialValue;
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

  @Override // IdGeneratorService
  public long getInitialValue() {
    return initialValue;
  }

  @Override // IdGeneratorService
  public void initialize() {
    client.start();
    try {
      id.initialize(initialValue);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start: " + name, e);
    }
  }

  @Override // IdGeneratorService
  public void close() {
    client.close();
  }
}
