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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.giraffa.hbase.INodeManager;
import org.apache.giraffa.hbase.NamespaceProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GiraffaTestUtils {

  private final static Logger LOG = LoggerFactory.getLogger(GiraffaTestUtils.class);

  public static final String BASE_TEST_DIRECTORY = "target/build/test-data";

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
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(
        HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY,
        DFSConfigKeys.FS_DEFAULT_NAME_DEFAULT);
    return new HBaseTestingUtility(conf);
  }

  public static INodeManager getNodeManager(HBaseTestingUtility util,
                                            GiraffaConfiguration conf) {
    TableName tableName =
        TableName.valueOf(conf.get(GiraffaConfiguration.GRFA_TABLE_NAME_KEY,
            GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT));
    HRegion hRegion = util.getHBaseCluster().getRegions(tableName).get(0);
    CoprocessorEnvironment env = hRegion.getCoprocessorHost()
        .findCoprocessorEnvironment(NamespaceProcessor.class.getName());
    return new INodeManager(conf, env);
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
