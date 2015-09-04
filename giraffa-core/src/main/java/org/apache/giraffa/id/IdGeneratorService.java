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
 * Id generator that connects to a service to store and compute values.
 * Implementations are expected to be thread-safe and increment atomically.
 */
public interface IdGeneratorService extends IdGenerator {

  /**
   * Return the id stored by this generator before any increments.
   */
  long getInitialValue();

  /**
   * Initialize the connection to the service.
   */
  void initialize();

  /**
   * Close the connection to the service.
   */
  void close();
}
