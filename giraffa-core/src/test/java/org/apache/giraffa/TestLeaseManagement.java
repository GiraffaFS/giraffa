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

import static org.apache.giraffa.GiraffaConstants.FileState;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.INodeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLeaseManagement {
  private static final Log LOG = LogFactory.getLog(TestLeaseManagement.class);

  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;
  private GiraffaConfiguration conf;
  private Connection connection;
  private INodeManager nodeManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseCommonTestingUtility.BASE_TEST_DIRECTORY_KEY,
        GiraffaTestUtils.BASE_TEST_DIRECTORY);
    Configuration hbaseConf = UTIL.getConfiguration();
    hbaseConf.setInt("hbase.assignment.maximum.attempts", 3);
    // Put meta on master to avoid meta server shutdown handling
    hbaseConf.set("hbase.balancer.tablesOnMaster", "hbase:meta");
    hbaseConf.setInt("hbase.master.maximum.ping.server.attempts", 3);
    hbaseConf.setInt("hbase.master.ping.server.retry.sleep.interval", 1);
    hbaseConf.setBoolean("hbase.assignment.usezk", false);
    hbaseConf.setBoolean(DFS_PERMISSIONS_ENABLED_KEY, false);
    GiraffaTestUtils.enableMultipleUsers();
    UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws IOException {
    conf = new GiraffaConfiguration(UTIL.getConfiguration());
    conf.setBoolean("fs.grfa.impl.disable.cache", true);
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
    connection = ConnectionFactory.createConnection(conf);
    nodeManager = GiraffaTestUtils.getNodeManager(conf, connection);
  }

  @After
  public void after() throws IOException {
    IOUtils.cleanup(LOG, grfs, nodeManager, connection);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLeaseCreation() throws IOException {
    String src = "/testLeaseCreation";
    Path path = new Path(src);
    long currentTime = Time.now();
    FSDataOutputStream outputStream = grfs.create(path);
    try {
      // check lease exists after creation
      checkLease(src, currentTime);
      // check lease renew
      currentTime = Time.now();
      grfs.grfaClient.getNamespaceService().renewLease(
          grfs.grfaClient.getClientName());
      checkLease(src, currentTime);
    } finally {
      IOUtils.closeStream(outputStream);
    }
    INodeFile iNode = INodeFile.valueOf(nodeManager.getINode(src));
    assertThat(iNode.getFileState(), is(FileState.CLOSED));
    FileLease lease = iNode.getLease();
    assertThat(lease, is(nullValue()));
  }

  @Test
  public void testLeaseFailure() throws IOException {
    String src = "/testLeaseFailure";
    Path path = new Path(src);
    long currentTime = Time.now();
    FSDataOutputStream outputStream = grfs.create(path);
    try {
      checkLease(src, currentTime);

      try {
        grfs.create(path, false);
        fail("Expected AlreadyBeingCreatedException");
      } catch (AlreadyBeingCreatedException e) {}

      // keep stream open intentionally
      checkLease(src, currentTime);
    } finally {
      IOUtils.closeStream(outputStream);
    }
    INodeFile iNode = INodeFile.valueOf(nodeManager.getINode(src));
    assertThat(iNode.getFileState(), is(FileState.CLOSED));
    FileLease lease = iNode.getLease();
    assertThat(lease, is(nullValue()));
  }

  @Test
  public void testLeaseRecovery() throws IOException {
    String src = "/testLeaseRecovery";
    Path path = new Path(src);

    HRegionServer server = UTIL.getHBaseCluster().getRegionServer(0);
    LeaseManager leaseManager = LeaseManager.originateSharedLeaseManager(
        server.getRpcServer().getListenerAddress().toString());

    FSDataOutputStream outputStream = grfs.create(path);
    String clientName = grfs.grfaClient.getClientName();
    outputStream.write(1);
    outputStream.write(2);
    outputStream.hflush();
    try {
      leaseManager.setHardLimit(10L);
      INodeFile iNode = null;
      for(int i = 0; i < 100; i++) {
        leaseManager.triggerLeaseRecovery();
        try {Thread.sleep(100L);} catch (InterruptedException ignored) {}
        iNode = INodeFile.valueOf(nodeManager.getINode(src));
        if(iNode.getFileState() == FileState.CLOSED)
          break;
      }
      assertThat(iNode.getFileState(), is(FileState.CLOSED));
      assertThat(iNode.getLen(), is(2L));
      assertThat(iNode.getLease(), is(nullValue()));
      assertThat(leaseManager.getLeases(clientName), is(nullValue()));
    } finally {
      leaseManager.setHardLimit(HdfsConstants.LEASE_HARDLIMIT_PERIOD);
      IOUtils.closeStream(outputStream);
    }
  }

  @Test
  public void testClientLeaseRecovery() throws IOException {
    String src = "/testLeaseRecovery";
    Path path = new Path(src);

    HRegionServer server = UTIL.getHBaseCluster().getRegionServer(0);
    LeaseManager leaseManager = LeaseManager.originateSharedLeaseManager(
        server.getRpcServer().getListenerAddress().toString());

    FSDataOutputStream outputStream = grfs.create(path);
    String clientName = grfs.grfaClient.getClientName();
    outputStream.write(1);
    outputStream.write(2);
    outputStream.hflush();
    try {
      boolean recovered = grfs.grfaClient.getNamespaceService().recoverLease(
          src, grfs.grfaClient.getClientName());
      assertThat(recovered, is(true));
      INodeFile iNode = INodeFile.valueOf(nodeManager.getINode(src));
      assertThat(iNode.getFileState(), is(FileState.CLOSED));
      assertThat(iNode.getLen(), is(2L));
      assertThat(iNode.getLease(), is(nullValue()));
      assertThat(leaseManager.getLeases(clientName), is(nullValue()));
    } finally {
      IOUtils.closeStream(outputStream);
    }
  }

  /**
   * This test shows that if a Region is to "migrate", either by split
   * or by RegionServer shutdown, that an incomplete file with a lease migrates
   * with the Region and that the lease is reloaded upon open and stays valid.
   */
  @Test
  public void testLeaseMigration() throws Exception {
    String src = "/testLeaseFailure";
    Path path = new Path(src);
    FSDataOutputStream outputStream = grfs.create(path);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    try {
      // keep stream open intentionally
      HRegionServer newServer = cluster.startRegionServer().getRegionServer();
      newServer.waitForServerOnline();
      HRegionServer dyingServer = cluster.getRegionServer(0);
      cluster.stopRegionServer(dyingServer.getServerName());
      cluster.waitForRegionServerToStop(dyingServer.getServerName(), 10000L);

      INodeFile iNode = null;
      do {
        try {
          IOUtils.cleanup(LOG, connection);
          connection = ConnectionFactory.createConnection(conf);
          IOUtils.cleanup(LOG, nodeManager);
          nodeManager = GiraffaTestUtils.getNodeManager(conf, connection);
          iNode = INodeFile.valueOf(nodeManager.getINode(src));
        } catch (ConnectException ignored) {}
      } while(iNode == null);

      FileLease rowLease = iNode.getLease();
      LeaseManager leaseManager = LeaseManager.originateSharedLeaseManager(
          newServer.getRpcServer().getListenerAddress().toString());
      Collection<FileLease> leases =
          leaseManager.getLeases(rowLease.getHolder());
      assertThat(leases.size(), is(1));
      FileLease leaseManagerLease = leases.iterator().next();
      // The following asserts are here to highlight that as a result of
      // migrating the FileLease across RegionServers we lose expiration date
      // consistency between the row field and the LeaseManager.
      assertThat(rowLease, is(not(equalTo(leaseManagerLease))));
      assertThat(rowLease.getHolder(),
          is(equalTo(leaseManagerLease.getHolder())));
      assertThat(rowLease.getPath(), is(equalTo(leaseManagerLease.getPath())));
      assertThat(rowLease.getLastUpdate(),
          is(not(equalTo(leaseManagerLease.getLastUpdate()))));
      // Renewing the lease restores the consistency.
      grfs.grfaClient.getNamespaceService().renewLease(
          grfs.grfaClient.getClientName());
      iNode = INodeFile.valueOf(nodeManager.getINode(src));
      rowLease = iNode.getLease();
      leases = leaseManager.getLeases(rowLease.getHolder());
      assertThat(leases.size(), is(1));
      leaseManagerLease = leases.iterator().next();
      assertThat(rowLease, is(equalTo(leaseManagerLease)));
    } finally {
      IOUtils.cleanup(LOG, outputStream);
    }
    INodeFile iNode = INodeFile.valueOf(nodeManager.getINode(src));
    assertThat(iNode.getFileState(), is(FileState.CLOSED));
    FileLease lease = iNode.getLease();
    assertThat(lease, is(nullValue()));
  }

  void checkLease(String src, long currentTime) throws IOException {
    INodeFile iNode = INodeFile.valueOf(nodeManager.getINode(src));
    FileLease lease = iNode.getLease();
    assertThat(iNode.getFileState(),
        is(FileState.UNDER_CONSTRUCTION));
    assertThat(lease, is(notNullValue()));
    assertThat(lease.getHolder(), is(grfs.grfaClient.getClientName()));
    assertThat(lease.getPath(), is(src));
    assertThat(lease.getLastUpdate() >= currentTime, is(true));
  }
}
