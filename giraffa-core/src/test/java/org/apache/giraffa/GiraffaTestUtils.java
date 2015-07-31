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

import static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import mockit.Mock;
import mockit.MockUp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.INodeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hdfs.DFSConfigKeys;

public class GiraffaTestUtils {
  static final Log LOG = LogFactory.getLog(GiraffaTestUtils.class);

  public static final String BASE_TEST_DIRECTORY = "target/build/test-data";

  private static MockUp<HBaseTestingUtility> utilMock;

  public static URI getGiraffaURI() throws IOException {
    try {
      return new URI(GiraffaConfiguration.GRFA_URI_SCHEME + ":///");
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public static void setGiraffaURI(Configuration conf) throws IOException {
    FileSystem.setDefaultUri(conf, getGiraffaURI());
  }

  public static HBaseTestingUtility getHBaseTestingUtility() {
    disableMultipleUsers();
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(
        HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY,
        DFSConfigKeys.FS_DEFAULT_NAME_DEFAULT);
    return new HBaseTestingUtility(conf);
  }

  /**
   * Prevents '.hfs.0' from being appending to the User name, allowing
   * permissions to be enabled in Giraffa but disallowing multiple RegionServers
   * in the test. Multiple users are disabled by default when you call
   * {@link #getHBaseTestingUtility()}.
   */
  public static void disableMultipleUsers() {
    if (utilMock == null) {
      utilMock = new MockUp<HBaseTestingUtility>() {
        @Mock
        public User getDifferentUser(Configuration c,
                                     String differentiatingSuffix)
            throws IOException {
          return User.getCurrent();
        }
      };
    }
  }

  /**
   * Appends a differentiating suffix to the User name for each configured
   * RegionServer. Enabling this allows multiple RegionServers to be configured
   * but disallows permission checking in Giraffa. Be sure to manually disable
   * permission checking in the configurations as this is not done by default.
   * This method should be invoked after {@link #getHBaseTestingUtility()}.
   */
  public static void enableMultipleUsers() {
    if (utilMock != null) {
      utilMock.tearDown();
      utilMock = null;
    }
  }

  /**
   * Returns an INodeManager that is incapable of opening and closing multiple
   * times because it has no coprocessor environment bound to it.
   * @throws IOException
   */
  public static INodeManager getNodeManager(GiraffaConfiguration conf,
                                            Connection connection,
                                            RowKeyFactory keyFactory)
      throws IOException {
    TableName tableName =
        TableName.valueOf(getGiraffaTableName(conf));
    Table table = connection.getTable(tableName);
    return new INodeManager(keyFactory, table);
  }

  static void listStatusRecursive(
      ArrayList<FileStatus> results, FileSystem fs, Path f
      ) throws IOException {
    FileStatus listing[] = fs.listStatus(f);

    for(FileStatus file : listing) {
      results.add(file);
      if(file.isDirectory())
        listStatusRecursive(results, fs, file.getPath());
    }
  }

  public static FileStatus[] listStatusRecursive(FileSystem fs, Path f)
      throws IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus> ();
    listStatusRecursive(results, fs, f);
    return results.toArray(new FileStatus[results.size()]);
  }

  public static void printFileStatus(FileStatus fileStat) throws IOException {
    printFileStatus(fileStat, -1);
  }

  public static void printFileStatus(FileStatus fileStat, int i)
      throws IOException {
    LOG.debug(
        "=============== FILE STATUS " + (i>=0?i:"") + " ===============");
    LOG.debug("OWNER: " + fileStat.getOwner());
    LOG.debug("GROUP: " + fileStat.getGroup());
    LOG.debug("PATH: " + fileStat.getPath());
    LOG.debug("PERMS: " + fileStat.getPermission().toString());
    LOG.debug("LEN: " + fileStat.getLen());
    LOG.debug("ATIME: " + fileStat.getAccessTime());
    LOG.debug("MTIME: " + fileStat.getModificationTime());
    LOG.debug("BLKSIZE: " + fileStat.getBlockSize());
    LOG.debug("REPL: " + fileStat.getReplication());
    if(fileStat.isSymlink())
      LOG.debug("SYMLINK: " + fileStat.getSymlink());
  }

  public static void printFileStatus(FileStatus[] fileStat) throws IOException {
    for (int i = 0; i < fileStat.length; i++) {
      printFileStatus(fileStat[i], i);
    }
  }
}
