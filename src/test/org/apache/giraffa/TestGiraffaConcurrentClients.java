package org.apache.giraffa;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.fs.FileSystem;
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
  private static final String BASE_TEST_DIRECTORY = "build/test-data";
  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private static MiniHBaseCluster cluster;
  private GiraffaFileSystem grfa;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup MiniCluster properties
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, BASE_TEST_DIRECTORY);
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

  private class ConcurrentClient implements Runnable
  {
    private boolean randomTopDir = true;
    private boolean completed = false;

    public void run() {
      String topDir = "/";
      if(randomTopDir) {
        Long randLong = new Random().nextLong();
        topDir = topDir + randLong + "/";
      }
      DFSTestUtil fsUtil = new DFSTestUtil("test", 100, 5, 1024);
      try {
        fsUtil.createFiles(grfa, topDir);
        completed = fsUtil.checkFiles(grfa, topDir);
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

  private void runConcurrentClients(int clientsToRun, boolean randomTopDir) {
    ConcurrentClient[] clients = new ConcurrentClient[clientsToRun];
    Thread[] threads = new Thread[clientsToRun];
    for(int i = 0; i < clientsToRun; i++) {
      ConcurrentClient cc = new ConcurrentClient();
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
  public void testFiveWithRandomRoot() throws IOException {
    runConcurrentClients(5, true);
  }

  @Test
  public void testFiveWithSameRoot() throws IOException {
    runConcurrentClients(5, false);
  }

  public static void main(String[] args) throws IOException {
    TestGiraffaConcurrentClients test = new TestGiraffaConcurrentClients();
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    test.grfa = (GiraffaFileSystem) FileSystem.get(conf);
    test.testFiveWithSameRoot();
    test.after();
  }
}
