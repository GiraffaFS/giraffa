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
package org.apache.giraffa.web;

import org.apache.giraffa.hbase.GiraffaConfiguration;
import org.apache.giraffa.GiraffaConstants;
import org.apache.giraffa.GiraffaFileSystem;
import org.apache.giraffa.GiraffaTestUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.apache.giraffa.GiraffaTestUtils.printFileStatus;
import static org.junit.Assert.assertEquals;

public class GiraffaWebDemoRunner {

  private static MiniHBaseCluster cluster;
  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;

  public static void main(String[] args) throws Exception {

    UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        GiraffaWebObserver.class.getName());
    System.setProperty(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY,
        GiraffaTestUtils.BASE_TEST_DIRECTORY);

    GiraffaWebDemoRunner webDemoRunner = new GiraffaWebDemoRunner();
    webDemoRunner.startCluster();
    webDemoRunner.initializeMockData();

    String cmd = null;
    //  open up standard input
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in,
        GiraffaConstants.UTF8));
    do {
      System.out.println("Enter 'stop' to shutdown:");
      try {
        cmd = br.readLine();
      } catch (IOException ioe) {
        System.out.println("IO error trying to read command!");
        System.exit(1);
      }
    } while (cmd == null || !cmd.equals("stop"));

    webDemoRunner.shutdown();
    System.exit(0);
  }

  private void startCluster() throws Exception {
    cluster = UTIL.startMiniCluster(1);

    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
  }

  public void shutdown() throws IOException {
    grfs.close();
    cluster.shutdown();
  }

  private void initializeMockData() throws IOException {
    grfs.create(new Path("/text.txt"));
    grfs.create(new Path("/kostya's test"));
    FileStatus[] files = grfs.listStatus(new Path("/"));
    printFileStatus(files);
    assertEquals(3, files.length);

    Path file = new Path("/writingA.txt");
    FSDataOutputStream out = grfs.create(file, true, 5000, (short) 3, 512);
    for (int i = 0; i < 2000; i++) {
      out.write('A');
    }
    out.close();

    Path bigFile = new Path("/writingBigFile512Block.txt");
    FSDataOutputStream out2 = grfs.create(bigFile, true, 5000, (short) 3, 512);
    for (int i = 0; i < 6742; i++) {
      out2.write('A');
    }
    out2.close();

  }


}
