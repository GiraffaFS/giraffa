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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.util.SequentialNumber;
import org.junit.Test;

public class TestSegmentedId {

  @Test
  public void testIncrement() {
    final long initialValue = 1000L;
    final long maxValue = 10000000L;
    LocalSegmentedId id = new LocalSegmentedId(initialValue);
    id.start();
    for (long i = initialValue + 1; i <= maxValue; i++) {
      assertThat(id.nextValue(), is(i));
    }
  }

  private static class LocalSegmentedId extends SegmentedId {

    LocalSegmentedId(long initialValue) {
      super(initialValue, new LocalSequentialId(-1));
    }
  }

  private static class LocalSequentialId extends SequentialNumber
      implements DistributedId {

    LocalSequentialId(long initialValue) {
      super(initialValue);
    }

    @Override // DistributedId
    public void start() {}

    @Override // DistributedId
    public void close() {}
  }
}
