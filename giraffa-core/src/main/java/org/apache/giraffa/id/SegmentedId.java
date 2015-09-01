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
package org.apache.giraffa.id;

import com.google.common.annotations.VisibleForTesting;

import org.apache.giraffa.hbase.ZookeeperId;
import org.apache.hadoop.conf.Configuration;

abstract class SegmentedId implements DistributedId {

  private static final long SEGMENT_SIZE = 1000;

  private final long initialValue;
  private final DistributedId segment;

  private long offset;
  private long value;

  SegmentedId(String name,
              Configuration conf,
              long initialValue) {
    this(initialValue, new ZookeeperId(name, conf, -1));
  }

  /** segment must have initial value of -1 */
  @VisibleForTesting
  SegmentedId(long initialValue,
              DistributedId segment) {
    this.initialValue = initialValue;
    this.segment = segment;
  }

  @Override // IdGenerator
  public synchronized long nextValue() {
    if (++offset == SEGMENT_SIZE) {
      offset = 0;
      newSegment();
      return value;
    }
    return ++value;
  }

  @Override // DistributedId
  public synchronized void start() {
    segment.start();
    newSegment();
  }

  @Override // DistributedId
  public synchronized void close() {
    segment.close();
  }

  private synchronized void newSegment() {
    value = segment.nextValue() * SEGMENT_SIZE + initialValue;
  }
}
