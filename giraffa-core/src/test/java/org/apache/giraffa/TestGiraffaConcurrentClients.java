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
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

public class TestGiraffaConcurrentClients {

  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private static MiniHBaseCluster cluster;
  private GiraffaFileSystem grfa;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup MiniCluster properties
    System.setProperty(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY,
        GiraffaTestUtils.BASE_TEST_DIRECTORY);
    cluster = UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws IOException {
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfa = (GiraffaFileSystem) FileSystem.get(conf);
  }

  @After
  public void after() throws IOException {
    if(grfa != null) grfa.close();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (cluster != null) cluster.shutdown();
  }

  private enum OPERATION { CREATE, DIR_LISTING }
  private class ConcurrentClient implements Runnable
  {
    private boolean randomTopDir = true;
    private boolean completed = false;
    private OPERATION op;

    public ConcurrentClient(OPERATION op) {
      this.op = op;
    }

    public void run() {
      String topDir = ".";
      if(randomTopDir) {
        Long randLong = new Random().nextLong();
        topDir = topDir + randLong + "/";
      }
      switch (op) {
        case CREATE:
          createAndCheckFiles(topDir);
          break;
        case DIR_LISTING:
          mkdirAndListDirs(topDir);
          break;
        default:
          throw new RuntimeException("Unknown operation: " + op);
      }
    }

    private void createAndCheckFiles(String topDir) {
      DFSTestUtil fsUtil = new DFSTestUtil("test", 100, 5, 1024, 0);
      try {
        fsUtil.createFiles(grfa, topDir);
        completed = fsUtil.checkFiles(grfa, topDir);
      } catch (IOException e) {
        e.printStackTrace();
        fail();
      }
    }

    private void mkdirAndListDirs(String topDir) {
      try {
        Path topDirPath = new Path(topDir);
        grfa.mkdirs(topDirPath);
        for(int i = 0; i < 128; i++) {
          grfa.mkdirs(new Path(topDirPath, "dir" + i));
        }
        long timeStarted = System.currentTimeMillis();
        FileStatus[] fileStatuses = grfa.listStatus(topDirPath);
        long timeEnded = System.currentTimeMillis();
        System.out.println(Arrays.toString(fileStatuses));
        System.out.println("Time started: " + timeStarted +
            ", topDir:" + topDir);
        System.out.println("Time ended: " + timeEnded +
            ", topDir:" + topDir);
        System.out.println(fileStatuses.length);
        completed = fileStatuses.length == 128;
      } catch (IOException e) {
        e.printStackTrace();
        fail();
      }
    }

    public void setRandomTopDir(boolean randomTopDir) {
      this.randomTopDir = randomTopDir;
    }

    public boolean isComplete() {
      return completed;
    }
  }

  private void runConcurrentClients(int clientsToRun, OPERATION op,
                                    boolean randomTopDir) {
    ConcurrentClient[] clients = new ConcurrentClient[clientsToRun];
    Thread[] threads = new Thread[clientsToRun];
    for(int i = 0; i < clientsToRun; i++) {
      ConcurrentClient cc = new ConcurrentClient(op);
      cc.setRandomTopDir(randomTopDir);
      clients[i] = cc;
      Thread thread = new Thread(cc);
      thread.start();
      threads[i] = thread;
    }
    for(int i = 0; i < clientsToRun; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail();
      }
      assertTrue(clients[i].isComplete());
    }
  }

  @Test
  public void testFiveCreationWithRandomRoot() throws IOException {
    runConcurrentClients(5, OPERATION.CREATE, true);
  }

  @Test
  public void testFiveCreationWithSameRoot() throws IOException {
    runConcurrentClients(5, OPERATION.CREATE, false);
  }

  @Test
  public void testFiveListingWithRandomRoot() throws IOException {
    runConcurrentClients(5, OPERATION.DIR_LISTING, true);
  }

  @Test
  public void testFiveListingWithSameRoot() throws IOException {
    runConcurrentClients(5, OPERATION.DIR_LISTING, false);
  }

  public static void main(String[] args) throws IOException {
    TestGiraffaConcurrentClients test = new TestGiraffaConcurrentClients();
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    test.grfa = (GiraffaFileSystem) FileSystem.get(conf);
    test.testFiveCreationWithSameRoot();
    test.after();
  }
}
