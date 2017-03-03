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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestGiraffaConcurrentClients {
  static final Log LOG = LogFactory.getLog(TestGiraffaConcurrentClients.class);

  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup MiniCluster properties
    System.setProperty(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY,
        GiraffaTestUtils.BASE_TEST_DIRECTORY);
    UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws IOException {
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
  }

  @After
  public void after() throws IOException {
    IOUtils.cleanup(LOG, grfs);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
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
        fsUtil.createFiles(grfs, topDir);
        completed = fsUtil.checkFiles(grfs, topDir);
      } catch (IOException e) {
        LOG.error("Failed", e);
        fail();
      }
    }

    private void mkdirAndListDirs(String topDir) {
      try {
        Path topDirPath = new Path(topDir);
        grfs.mkdirs(topDirPath);
        for(int i = 0; i < 128; i++) {
          grfs.mkdirs(new Path(topDirPath, "dir" + i));
        }
        long timeStarted = System.currentTimeMillis();
        FileStatus[] fileStatuses = grfs.listStatus(topDirPath);
        long timeEnded = System.currentTimeMillis();
        LOG.debug(Arrays.toString(fileStatuses));
        LOG.debug("Time started: " + timeStarted +
            ", topDir:" + topDir);
        LOG.debug("Time ended: " + timeEnded +
            ", topDir:" + topDir);
        LOG.debug("files: " + fileStatuses.length);
        completed = fileStatuses.length == 128;
      } catch (IOException e) {
        LOG.error("Failed", e);
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
        LOG.error("Failed", e);
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
    test.grfs = (GiraffaFileSystem) FileSystem.get(conf);
    test.testFiveCreationWithSameRoot();
    test.after();
  }
}
