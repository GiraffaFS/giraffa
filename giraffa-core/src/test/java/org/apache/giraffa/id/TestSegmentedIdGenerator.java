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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.util.SequentialNumber;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestSegmentedIdGenerator {

  @Test
  public void testIncrement() {
    final long initialValue = 1000;
    final long serviceInitialValue = -1;
    final long maxValue = 10000000;

    IdGeneratorService service = new LocalIdService(serviceInitialValue);
    SegmentedIdGenerator id = new SegmentedIdGenerator(initialValue, service);
    for (long i = initialValue + 1; i <= maxValue; i++) {
      assertThat(id.nextValue(), is(i));
    }
  }

  @Test
  public void testConcurrentIncrement() throws InterruptedException {
    final long initialValue = 1000;
    final long serviceInitialValue = -1;
    final int numTesters = 3;
    final int increments = 1000000;

    // creater tester threads
    IdTester[] testers = new IdTester[numTesters];
    Thread[] threads = new Thread[numTesters];
    IdGeneratorService service = new LocalIdService(serviceInitialValue);
    for (int i = 0; i < numTesters; i++) {
      SegmentedIdGenerator id = new SegmentedIdGenerator(initialValue, service);
      testers[i] = new IdTester(id, increments);
      threads[i] = new Thread(testers[i]);
    }

    // start threads and wait for them to finish
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }

    // check ids generated uniquely and increasingly
    List<Long> generated = new ArrayList<>();
    for (IdTester tester : testers) {
      checkIncreasing(tester.generated);
      generated.addAll(tester.generated);
    }
    Collections.sort(generated);
    assertThat(generated.get(0), is(initialValue + 1));
    checkIncreasing(generated);
  }

  private void checkIncreasing(List<Long> list) {
    for (int i = 1; i < list.size(); i++) {
      assertThat(list.get(i), greaterThan(list.get(i - 1)));
    }
  }

  /** each instance will repeatedly generate ids */
  private static class IdTester implements Runnable {

    final SegmentedIdGenerator id;
    final int increments;
    final List<Long> generated;

    IdTester(SegmentedIdGenerator id, int increments) {
      this.id = id;
      this.increments = increments;
      this.generated = new ArrayList<>();
    }

    @Override // Runnable
    public void run() {
      for (int i = 0; i < increments; i++) {
        generated.add(id.nextValue());
      }
    }
  }

  private static class LocalIdService extends SequentialNumber
      implements IdGeneratorService {

    private final long initialValue;

    LocalIdService(long initialValue) {
      super(initialValue);
      this.initialValue = initialValue;
    }

    @Override
    public long getInitialValue() {
      return initialValue;
    }

    @Override // IdGeneratorService
    public void initialize() {}

    @Override // IdGeneratorService
    public void close() {}
  }
}
