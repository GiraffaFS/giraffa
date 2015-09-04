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

import org.apache.hadoop.util.IdGenerator;

/**
 * ID generator that allocates large segments of sequential ids at a time from a
 * given {@link IdGeneratorService} to minimize calls to the service. This class
 * is thread-safe as long as the underlying generator is thread-safe.
 */
public class SegmentedIdGenerator implements IdGenerator {

  private static final long SEGMENT_SIZE = 1000;

  private final long initialValue;
  private final IdGeneratorService service;

  private long offset;
  private long value;

  public SegmentedIdGenerator(long initialValue,
                              IdGeneratorService service) {
    this.initialValue = initialValue;
    this.service = service;
  }

  @Override // IdGenerator
  public synchronized long nextValue() {
    if (offset == 0) {
      newSegment();
    }
    if (++offset == SEGMENT_SIZE){
      offset = 0;
    }
    return ++value;
  }

  private synchronized void newSegment() {
    long segmentOffset = service.nextValue() - service.getInitialValue() - 1;
    value = segmentOffset * SEGMENT_SIZE + initialValue;
  }
}
