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
package org.apache.giraffa;

import static org.apache.giraffa.GiraffaConfiguration.GRFA_CACHING_DEFAULT;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_CACHING_KEY;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_ROWKEY_FACTORY_DEFAULT;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_ROWKEY_FACTORY_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class RowKeyFactoryProvider {

  private static Class<? extends RowKeyFactory> rowKeyFactoryClass;

  public static RowKeyFactory createFactory(Configuration conf)
      throws IOException {
    boolean caching = conf.getBoolean(GRFA_CACHING_KEY, GRFA_CACHING_DEFAULT);
    RowKeyFactory.setCache(caching);
    Class<? extends RowKeyFactory> rkfClass =  registerFactory(conf);
    RowKeyFactory rkf = ReflectionUtils.newInstance(rkfClass, conf);
    rkf.initialize(conf);
    rowKeyFactoryClass = rkfClass;
    return rkf;
  }

  @SuppressWarnings("unchecked")
  private static synchronized Class<? extends RowKeyFactory> registerFactory(Configuration conf)
      throws IOException {
    Class<? extends RowKeyFactory> factory;
    try {
      if(rowKeyFactoryClass != null)
        return rowKeyFactoryClass;
      factory = (Class<? extends RowKeyFactory>) conf.getClass(
          GRFA_ROWKEY_FACTORY_KEY, GRFA_ROWKEY_FACTORY_DEFAULT);
    } catch(Exception e) {
      throw new IOException("Error retrieving RowKeyFactory class", e);
    }
    return factory;
  }
}
