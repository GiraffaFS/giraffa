/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraffa.hbase;

import java.io.IOException;

import org.apache.giraffa.GiraffaConfiguration;
import org.apache.giraffa.GiraffaFileSystem;
import org.apache.giraffa.GiraffaTestUtils;
import org.apache.giraffa.INode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.fs.permission.FsAction.WRITE;
import static org.apache.hadoop.fs.permission.FsAction.WRITE_EXECUTE;
import static org.junit.Assert.fail;

/**
 * Unit tests covering FSPermissionChecker.  All tests in this suite have been
 * cross-validated against Linux setfacl/getfacl to check for consistency of the
 * HDFS implementation.
 */
@SuppressWarnings("OctalInteger") public class TestFsPermissionChecker {
  private static final long PREFERRED_BLOCK_SIZE = 128 * 1024 * 1024;
  private static final short REPLICATION = 3;
  private static final UserGroupInformation SUPER =
      UserGroupInformation.createUserForTesting(FsPermissionChecker.FS_OWNER,
          new String[] {FsPermissionChecker.FS_SUPERGROUP});
  private static final UserGroupInformation BRUCE =
      UserGroupInformation.createUserForTesting("bruce", new String[] {});
  private static final UserGroupInformation DIANA =
      UserGroupInformation
          .createUserForTesting("diana", new String[] { "sales" });
  private static final UserGroupInformation CLARK =
      UserGroupInformation
          .createUserForTesting("clark", new String[] { "execs" });

  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();

  private GiraffaFileSystem grfs;
  private INodeManager nodeManager;
  private INode inodeRoot;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseCommonTestingUtility.BASE_TEST_DIRECTORY_KEY,
        GiraffaTestUtils.BASE_TEST_DIRECTORY);
    UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws IOException {
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
    nodeManager = GiraffaTestUtils.getNodeManager(UTIL, conf);
    grfs.mkdirs(new Path("/"));
    inodeRoot = nodeManager.getINode("/");
    UserGroupInformation.setLoginUser(SUPER);
  }

  @After
  public void after() throws IOException {
    if (grfs != null) grfs.close();
    nodeManager.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAclOwner() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "execs",
        (short) 0640);
    assertPermissionGranted(BRUCE, "/file1", READ);
    assertPermissionGranted(BRUCE, "/file1", WRITE);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionDenied(BRUCE, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
  }

  @Test
  public void testAclNamedUser() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "execs",
        (short) 0640);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclNamedUserDeny() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "execs",
        (short) 0644);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", READ);
  }

  @Test
  public void testAclNamedUserTraverseDeny() throws IOException {
    INode inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
        "execs", (short) 0755);
    createINode(inodeDir, "file1", "bruce", "execs",
        (short) 0644);
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }

  @Test
  public void testAclNamedUserMask() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "execs",
        (short) 0620);
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclGroup() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "execs",
        (short) 0640);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  @Test
  public void testAclGroupDeny() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "sales",
        (short) 0604);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclGroupTraverseDeny() throws IOException {
    INode inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
        "execs", (short) 0755);
    createINode(inodeDir, "file1", "bruce", "execs",
        (short) 0644);
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", ALL);
  }

  @Test
  public void testAclGroupTraverseDenyOnlyDefaultEntries() throws IOException {
    INode inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
        "execs", (short) 0755);
    createINode(inodeDir, "file1", "bruce", "execs",
        (short) 0644);
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", ALL);
  }

  @Test
  public void testAclGroupMask() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "execs",
        (short) 0644);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  @Test
  public void testAclNamedGroup() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "execs",
        (short) 0640);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclNamedGroupDeny() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "sales",
        (short) 0644);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  @Test
  public void testAclNamedGroupTraverseDeny() throws IOException {
    INode inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
        "execs", (short) 0755);
    createINode(inodeDir, "file1", "bruce", "execs",
        (short) 0644);
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }

  @Test
  public void testAclNamedGroupMask() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "execs",
        (short) 0644);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclOther() throws IOException {
    createINode(inodeRoot, "file1", "bruce", "sales",
        (short) 0774);
    assertPermissionGranted(BRUCE, "/file1", ALL);
    assertPermissionGranted(DIANA, "/file1", ALL);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  private void assertPermissionGranted(UserGroupInformation user, String path,
      FsAction access) throws IOException {
    new FsPermissionChecker(user, getResolver()).checkPermission(
        path,
        false, null, null, access, null, false, true);
  }

  private FsPermissionChecker.INodeResolver getResolver() {
    return FsPermissionChecker.getResolver(nodeManager);
  }

  private void assertPermissionDenied(UserGroupInformation user, String path,
      FsAction access) throws IOException {
    try {
      new FsPermissionChecker(user, getResolver()).checkPermission(path,
          false, null, null, access, null, false, true);
      fail("expected AccessControlException for user + " + user + ", path = " +
          path + ", access = " + access);
    } catch (AccessControlException e) {
      // expected
    }
  }

  private INode createINodeDirectory(INode parent,
      String name, String owner, String group, short perm) throws IOException {
    Path p = new Path(parent.getRowKey().getPath(), name);
    grfs.mkdirs(p,
        new FsPermission(perm));
    grfs.setOwner(p, owner, group);
    return nodeManager.getINode(p.toString());
  }

  private INode createINode(INode parent, String name,
      String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
        new FsPermission(perm));
    Path p = new Path(parent.getRowKey().getPath(), name);
    grfs.create(p,
        permStatus.getPermission(), true,
        1024, REPLICATION, PREFERRED_BLOCK_SIZE, null).close();
    grfs.setOwner(p, owner, group);
    return nodeManager.getINode(p.toString());
  }
}
