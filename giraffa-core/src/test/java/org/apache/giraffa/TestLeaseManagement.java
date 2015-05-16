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

import java.util.Collection;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.giraffa.hbase.INodeManager;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLeaseManagement {
  private static MiniHBaseCluster cluster;
  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;
  private INodeManager nodeManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseCommonTestingUtility.BASE_TEST_DIRECTORY_KEY,
        GiraffaTestUtils.BASE_TEST_DIRECTORY);
    cluster = UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws IOException {
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
    CoprocessorEnvironment env = new CoprocessorHost.Environment(
        null, 0, 0, cluster.getConfiguration());
    nodeManager = new INodeManager(conf, env);
  }

  @After
  public void after() throws IOException {
    if(grfs != null) grfs.close();
    nodeManager.close();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (cluster != null) cluster.shutdown();
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
    INode iNode = nodeManager.getINode(src);
    assertThat(iNode.getFileState(), is(GiraffaConstants.FileState.CLOSED));
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
    INode iNode = nodeManager.getINode(src);
    assertThat(iNode.getFileState(), is(GiraffaConstants.FileState.CLOSED));
    FileLease lease = iNode.getLease();
    assertThat(lease, is(nullValue()));
  }

  /**
   * This test shows that if a Region is to "migrate", either by split
   * or by RegionServer shutdown, that an incomplete file with a lease migrates
   * with the Region and that the lease is reloaded upon open and stays valid.
   */
  @Test
  public void testLeaseMigration() throws IOException {
    String src = "/testLeaseFailure";
    Path path = new Path(src);
    FSDataOutputStream outputStream = grfs.create(path);
    try {
      // keep stream open intentionally
      cluster.stopRegionServer(0);
      JVMClusterUtil.RegionServerThread regionServerThread =
          cluster.startRegionServer();
      regionServerThread.waitForServerOnline();

      INode iNode = nodeManager.getINode(src);
      FileLease rowLease = iNode.getLease();
      LeaseManager leaseManager =
          LeaseManager.originateSharedLeaseManager(
              regionServerThread.getRegionServer().getRpcServer()
                  .getListenerAddress());
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
      iNode = nodeManager.getINode(src);
      rowLease = iNode.getLease();
      leases = leaseManager.getLeases(rowLease.getHolder());
      assertThat(leases.size(), is(1));
      leaseManagerLease = leases.iterator().next();
      assertThat(rowLease, is(equalTo(leaseManagerLease)));
    } finally {
      IOUtils.closeStream(outputStream);
    }
    INode iNode = nodeManager.getINode(src);
    assertThat(iNode.getFileState(), is(GiraffaConstants.FileState.CLOSED));
    FileLease lease = iNode.getLease();
    assertThat(lease, is(nullValue()));
  }

  void checkLease(String src, long currentTime) throws IOException {
    INode iNode = nodeManager.getINode(src);
    FileLease lease = iNode.getLease();
    assertThat(iNode.getFileState(),
        is(GiraffaConstants.FileState.UNDER_CONSTRUCTION));
    assertThat(lease, is(notNullValue()));
    assertThat(lease.getHolder(), is(grfs.grfaClient.getClientName()));
    assertThat(lease.getPath(), is(src));
    assertThat(lease.getLastUpdate() >= currentTime, is(true));
  }
}
