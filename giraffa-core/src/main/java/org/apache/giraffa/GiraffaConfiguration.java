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

import org.apache.giraffa.hbase.NamespaceAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.ReflectionUtils;

public class GiraffaConfiguration extends Configuration {
  public static final String  GRFA_URI_SCHEME = "grfa";
  public static final String  GRFA_TABLE_NAME_KEY = "grfa.table.name";
  public static final String  GRFA_TABLE_NAME_DEFAULT = "Namespace";
  public static final String  GRFA_ROW_KEY_KEY = "grfa.rowkey.class";
  public static final Class<FileIdRowKey> GRFA_ROW_KEY_DEFAULT =
      FileIdRowKey.class;
  public static final String  GRFA_CACHING_KEY = "grfa.rowkey.caching";
  public static final Boolean GRFA_CACHING_DEFAULT = true;
  public static final String  GRFA_NAMESPACE_SERVICE_KEY = 
                                  "grfa.namespace.service.class"; 
  public static final Class<NamespaceAgent> GRFA_NAMESPACE_SERVICE_DEFAULT =
                                  NamespaceAgent.class;
  public static final String  GRFA_HDFS_ADDRESS_KEY = "grfa.hdfs.address";
  public static final String  GRFA_HDFS_ADDRESS_DEFAULT = "file:///";
  public static final String  GRFA_HBASE_ADDRESS_KEY = "grfa.hbase.address";
  public static final String  GRFA_HBASE_ADDRESS_DEFAULT = "file:///";
  public static final String  GRFA_LIST_LIMIT_KEY =
                                  DFSConfigKeys.DFS_LIST_LIMIT;
  public static final int     GRFA_LIST_LIMIT_DEFAULT =
                                  DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;


  public static final String  GRFA_WEB_ADDRESS_KEY = "grfa.http-address";
  public static final String  GRFA_WEB_ADDRESS_DEFAULT = "0.0.0.0:40010";

  static {
    // adds the default resources
    addDefaultResource("giraffa-default.xml");
    addDefaultResource("giraffa-site.xml");
    addDefaultResource("hbase-default.xml");
    addDefaultResource("hbase-site.xml");
  }

  public GiraffaConfiguration() {
    super();
  }

  public GiraffaConfiguration(Configuration conf) {
    super(conf);
  }

  public NamespaceService newNamespaceService() {
    Class<? extends NamespaceService> serviceClass =
        getClass(GRFA_NAMESPACE_SERVICE_KEY, GRFA_NAMESPACE_SERVICE_DEFAULT,
            NamespaceService.class);
    return ReflectionUtils.newInstance(serviceClass, null);
  }

  public static String getGiraffaTableName(Configuration conf) {
    return conf.get(GRFA_TABLE_NAME_KEY, GRFA_TABLE_NAME_DEFAULT);
  }
}
