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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSXAttrBaseTest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.hadoop.security.UserGroupInformation.createUserForTesting;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import mockit.Mock;
import mockit.MockUp;

/**
 * This file is dedicated to test extended attribute(XAttr) related methods
 * of GiraffaFileSystem.
 * Leverage legacy tests in FSXAttrBaseTest except testXAttrAcl since ACL
 * related methods are not supported now
 */
public class TestXAttr extends FSXAttrBaseTest {
  static final Log LOG = LogFactory.getLog(TestXAttr.class);

  private static final HBaseTestingUtility UTIL =
    GiraffaTestUtils.getHBaseTestingUtility();
  private Path path1;
  private Path path2;
  private Path noThisPath;
  private String attrName1;
  private String attrName2;
  private String attrName3;
  private String attrName4;
  private byte[] attrValue1;
  private byte[] attrValue2;
  private byte[] attrValue3;
  private byte[] attrValue4;
  // need another user since default one is super user
  private static UserGroupInformation user1;
  private static FileSystem user1fs;
  private Path user1Path;

  private class MockMiniDFSCluster extends MiniDFSCluster {
    private MockDistributedFileSystem dfs;

    public MockMiniDFSCluster() {
      dfs = new MockDistributedFileSystem();
    }

    @Override
    public DistributedFileSystem getFileSystem() throws IOException{
      return dfs;
    }
  }

  private class MockDistributedFileSystem extends DistributedFileSystem {
    @Override
    public byte[] getXAttr(Path path, final String name) throws IOException {
      return user1fs.getXAttr(path, name);
    }

    @Override
    public void removeXAttr(Path path, final String name) throws IOException {
      user1fs.removeXAttr(path, name);
    }
    @Override
    public List<String> listXAttrs(Path path) throws IOException {
      return user1fs.listXAttrs(path);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    dfsCluster.shutdown(); // which started in FSXAttrBaseTest
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY,
         GiraffaTestUtils.BASE_TEST_DIRECTORY);
    Configuration hbaseConf = UTIL.getConfiguration();
    hbaseConf.setInt(DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 3);
    hbaseConf.setInt(DFS_NAMENODE_MAX_XATTR_SIZE_KEY, 16);

    UTIL.startMiniCluster(1);

    conf = new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    user1 = createUserForTesting("user1", new String[]{"mygroup"});
    user1fs = getFS();

    new MockUp<FSXAttrBaseTest>() {
      @Mock
      void restart(boolean checkpoint) throws Exception {
        // Do nothing.
      }

      @Mock
      void initFileSystem() throws Exception {
        // Do nothing.
      }

      @Mock
      void shutdown() {
        // Do nothing.
      }
    };
  }

  @Before
  public void before() throws IOException {
    // File System Initialization
    GiraffaFileSystem.format((GiraffaConfiguration)conf, false);
    fs = FileSystem.get(conf);

    createFiles();
    setupForOtherUsers();
    initAttributes();
    dfsCluster = new MockMiniDFSCluster();
  }

  @After
  public void after() throws IOException {
    IOUtils.cleanup(LOG, fs);
  }

  @After
  @Override
  public void destroyFileSystems() {
    // Do nothing.
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IOUtils.cleanup(LOG, user1fs);
//    File uselessDirCreatedByBaseClass = new File("build");
//    FileUtil.fullyDelete(uselessDirCreatedByBaseClass);
    UTIL.shutdownMiniCluster();
  }

  @Override
  public void testXAttrAcl() throws Exception {
    // does not support ACL related methods so replace old one with empty test
  }

  /**
   * setXAttr related Tests
   */
  @Test (expected = FileNotFoundException.class)
  public void testCannotAddXAttrToNonExistedFile()
          throws IOException {
    fs.setXAttr(noThisPath, attrName1, attrValue1);
  }

  @Test (expected = NullPointerException.class)
  public void testCanNotSetXAttrWithNullPath() throws IOException {
    try {
      fs.setXAttr(null, attrName1, attrValue1);
      fail("expected NullPointerException");
    } finally {
      List<String> listOfXAttrNames = fs.listXAttrs(path1);
      assertEquals(0, listOfXAttrNames.size());
    }
  }

  @Test
  public void testCanOverwriteXAttrWithoutAdditionalFlag() throws IOException {
    fs.setXAttr(path1, attrName1, attrValue1);
    fs.setXAttr(path1, attrName1, attrValue2);
    List<String> listOfXAttrNames = fs.listXAttrs(path1);
    assertEquals(1, listOfXAttrNames.size());
    assertTrue(listOfXAttrNames.contains(attrName1));
    assertArrayEquals(attrValue2, fs.getXAttr(path1, attrName1));
  }

  @Test
  public void testCanOverwriteXAttrWithoutDestroyOtherXAttr()
      throws IOException {
    fs.setXAttr(path1, attrName1, attrValue1);
    fs.setXAttr(path1, attrName2, attrValue2);
    fs.setXAttr(path1, attrName1, attrValue3);
    List<String> listOfXAttrNames = fs.listXAttrs(path1);
    assertEquals(2, listOfXAttrNames.size());
    assertTrue(listOfXAttrNames.contains(attrName1));
    assertTrue(listOfXAttrNames.contains(attrName2));
    assertArrayEquals(attrValue3, fs.getXAttr(path1, attrName1));
    assertArrayEquals(attrValue2, fs.getXAttr(path1, attrName2));
  }

  @Test
  public void testAttrNamePrefixIsCaseInsensitive() throws IOException {
    String caseInsensitiveName = "uSeR.attr1";
    fs.setXAttr(path1, attrName1, attrValue1);
    fs.setXAttr(path1, caseInsensitiveName, attrValue2);
    List<String> listOfXAttrNames = fs.listXAttrs(path1);
    assertEquals(1, listOfXAttrNames.size());
    assertTrue(listOfXAttrNames.contains(attrName1));
    assertArrayEquals(attrValue2, fs.getXAttr(path1, caseInsensitiveName));
  }

  @Test (expected = NullPointerException.class)
  public void testCanNotAddNewXAttrWithNullFlag() throws IOException {
    try {
      fs.setXAttr(path1, attrName1, attrValue2, null);
      fail("expected NullPointerException");
    } finally {
      List<String> listOfXAttrNames = fs.listXAttrs(path1);
      assertEquals(0, listOfXAttrNames.size());
    }
  }

  @Test (expected = IOException.class)
  public void testCanNotAddNewXAttrWithEmptyFlag() throws IOException {
    try {
      fs.setXAttr(path1, attrName1, attrValue2,
              EnumSet.noneOf(XAttrSetFlag.class));
      fail("expected IOException");
    } finally {
      List<String> listOfXAttrNames = fs.listXAttrs(path1);
      assertEquals(0, listOfXAttrNames.size());
    }
  }

  /**
   * getXAttr related tests
   */
  @Test
  public void testCanGetXAttrValueByPathAndAttrName() throws IOException {
    fs.setXAttr(path1, attrName1, attrValue1);
    assertArrayEquals(attrValue1, fs.getXAttr(path1, attrName1));
  }

  @Test
  public void testCanGetMapOfXAttrByPathAndSingleElementAttrNameList()
          throws IOException {
    // one to one match
    fs.setXAttr(path1, attrName1, attrValue1);
    List<String> attrNameList = Collections.singletonList(attrName1);
    Map<String, byte[]> map = fs.getXAttrs(path1, attrNameList);
    assertEquals(1, map.size());
    assertTrue(map.containsKey(attrName1));
    assertArrayEquals(attrValue1, map.get(attrName1));

    fs.setXAttr(path1, attrName2, attrValue2);

    // now we have two attributes
    map = fs.getXAttrs(path1, attrNameList);
    assertEquals(1, map.size());
    assertTrue(map.containsKey(attrName1));
    assertArrayEquals(attrValue1, map.get(attrName1));

    attrNameList = Collections.singletonList(attrName2);
    map = fs.getXAttrs(path1, attrNameList);
    assertEquals(1, map.size());
    assertTrue(map.containsKey(attrName2));
    assertArrayEquals(attrValue2, map.get(attrName2));
  }

  @Test (expected = HadoopIllegalArgumentException.class)
  public void testCanNotGetMapOfXAttrByPathAndEmptyAttrNameList()
          throws IOException {
    fs.setXAttr(path1, attrName1, attrValue1);
    List<String> attrNameList = Collections.emptyList();
    fs.getXAttrs(path1, attrNameList);
  }

  @Test (expected = HadoopIllegalArgumentException.class)
  public void testCanNotGetMapOfXAttrByPathAndNullNameList()
          throws IOException {
    fs.setXAttr(path1, attrName1, attrValue1);
    fs.getXAttrs(path1, null);
  }

  @Test
  public void testCanGetMapOfXAttrByPathAndMultipleElementsAttrNameList()
          throws IOException {
    fs.setXAttr(path1, attrName1, attrValue1);
    fs.setXAttr(path1, attrName2, attrValue2);
    fs.setXAttr(path1, attrName3, attrValue3);
    fs.setXAttr(path2, attrName4, attrValue4);
    // check if interfere with each other
    List<String> attrNameList = Collections.singletonList(attrName4);
    Map<String, byte[]> map = fs.getXAttrs(path2, attrNameList);
    assertEquals(1, map.size());
    assertTrue(map.containsKey(attrName4));
    assertArrayEquals(attrValue4, map.get(attrName4));

    // check if partial match
    attrNameList = Arrays.asList(attrName2, attrName3);
    map = fs.getXAttrs(path1, attrNameList);
    assertEquals(2, map.size());
    assertTrue(map.containsKey(attrName2));
    assertArrayEquals(attrValue2, map.get(attrName2));
    assertTrue(map.containsKey(attrName3));
    assertArrayEquals(attrValue3, map.get(attrName3));

    // check if full match
    attrNameList = Arrays.asList(attrName1, attrName2, attrName3);
    map = fs.getXAttrs(path1, attrNameList);
    assertEquals(3, map.size());
    assertTrue(map.containsKey(attrName1));
    assertArrayEquals(attrValue1, map.get(attrName1));
    assertTrue(map.containsKey(attrName2));
    assertArrayEquals(attrValue2, map.get(attrName2));
    assertTrue(map.containsKey(attrName3));
    assertArrayEquals(attrValue3, map.get(attrName3));
  }

  @Test
  public void testCanGetXAttrValueMultipleTimes() throws IOException {
    fs.setXAttr(path1, attrName1, attrValue1);
    fs.setXAttr(path1, attrName2, attrValue2);
    fs.setXAttr(path2, attrName1, attrValue3);

    // byte[] getXAttr(String src, String name)
    assertArrayEquals(attrValue1, fs.getXAttr(path1, attrName1));
    assertArrayEquals(attrValue2, fs.getXAttr(path1, attrName2));
    assertArrayEquals(attrValue3, fs.getXAttr(path2, attrName1));
  }

  @Test
  public void testCanNotGetXAttrFromNonExistedFile() throws IOException {
    try {
      fs.getXAttr(noThisPath, attrName1);
      fail("expected FileNotFoundException");
    } catch (FileNotFoundException ignored) {}

    try {
      fs.getXAttrs(noThisPath);
      fail("expected FileNotFoundException");
    } catch (FileNotFoundException ignored) {}

    try {
      List<String> attrNameList = Collections.singletonList(attrName1);
      fs.getXAttrs(noThisPath, attrNameList);
      fail("expected FileNotFoundException");
    } catch (FileNotFoundException ignored) {}
  }

  @Test
  public void testCanNotGetXAttrWithNullPath() throws IOException {
    try {
      fs.getXAttr(null, attrName1);
      fail("expected NullPointerException");
    } catch (NullPointerException ignored) {}

    try {
      fs.getXAttrs(null);
      fail("expected NullPointerException");
    } catch (NullPointerException ignored) {}

    try {
      List<String> attrNameList = Collections.singletonList(attrName1);
      fs.getXAttrs(null, attrNameList);
      fail("expected NullPointerException");
    } catch (NullPointerException ignored) {}
  }

  @Test (expected = NullPointerException.class)
  public void testCanNotGetXAttrWithNullAttrName() throws IOException {
    fs.getXAttr(path1, null);
  }

  /**
   * removeXAttr related tests
   */
  @Test
  public void testCanRemoveAnXAttrWithMultipleVersion() throws IOException {
    fs.setXAttr(path1, attrName1, attrValue1);
    fs.setXAttr(path1, attrName1, attrValue2);
    fs.removeXAttr(path1, attrName1);
    assertEquals(0, fs.listXAttrs(path1).size());
  }

  @Test (expected = NullPointerException.class)
  public void testCanNotRemoveAnXAttrWithNullPath() throws IOException {
    fs.removeXAttr(null, attrName1);
  }

  @Test (expected = FileNotFoundException.class)
  public void testCanNotRemoveAnXAttrOnNonExistedFile() throws IOException {
    fs.removeXAttr(noThisPath, attrName1);
  }

  @Test (expected = NullPointerException.class)
  public void testCanNotRemoveAnXAttrWithNullAttrName() throws IOException {
    fs.removeXAttr(path1, null);
  }

  /**
   * permission related Tests
   */
  @Test
  public void testOnlySuperUserCanSetTRUSTEDXAttr() throws Exception {
    fs.setXAttr(path2, "trusted.a2", attrValue2);
    assertEquals(1, fs.listXAttrs(path2).size());

    createEmptyFile(user1fs, path1);
    try {
      user1fs.setXAttr(path1, "trusted.a1", attrValue2);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.
      assertExceptionContains("User doesn\'t have permission", e);
    }
    assertEquals(0, user1fs.listXAttrs(path1).size());
  }

  @Test
  public void testNoUserCanSetSYSTEMXAttr() throws Exception {
    try {
      fs.setXAttr(path2, "system.a2", attrValue2);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.
          assertExceptionContains("User doesn\'t have permission", e);
    }
    assertEquals(0, fs.listXAttrs(path2).size());

    createEmptyFile(user1fs, path1);
    try {
      user1fs.setXAttr(path1, "system.a1", attrValue1);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.
      assertExceptionContains("User doesn\'t have permission", e);
    }
    assertEquals(0, user1fs.listXAttrs(path1).size());
  }

  @Test
  public void testNoUserCanSetSECURITYXAttr() throws Exception {
    try {
      fs.setXAttr(path2, "security.a2", attrValue2);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.
          assertExceptionContains("User doesn\'t have permission", e);
    }
    assertEquals(0, fs.listXAttrs(path2).size());

    createEmptyFile(user1fs, path1);
    try {
      user1fs.setXAttr(path1, "security.a1", attrValue1);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.
      assertExceptionContains("User doesn\'t have permission", e);
    }
    assertEquals(0, user1fs.listXAttrs(path1).size());
  }

  @Test
  public void testCanNotSetXAttrWithoutWPermission() throws Exception {
    createEmptyFile(user1fs, path1);
    user1fs.setPermission(path1,
        FsPermission.createImmutable((short) 352));
    try {
      user1fs.setXAttr(path1, attrName1, attrValue1);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }
    assertEquals(0, fs.listXAttrs(user1Path).size());
  }

  @Test
  public void testCanNotSetXAttrWithoutParentXPermission() throws Exception {
    createEmptyFile(user1fs, path1);

    // remove parent node's X permission
    Path path1Parent = new Path("aaa/bbb");
    user1fs.setPermission(path1Parent,
        FsPermission.createImmutable((short) 384));
    try {
      user1fs.setXAttr(path1, attrName1, attrValue1);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }
    assertEquals(0, fs.listXAttrs(user1Path).size());
  }

  @Test
  public void testOnlySuperUserCanGetOrListTRUSTEDXAttr() throws Exception {
    createEmptyFile(user1fs, path1);
    user1fs.setXAttr(path1, attrName1, attrValue1);
    assertEquals(1, user1fs.getXAttrs(path1).size());
    assertEquals(1, user1fs.listXAttrs(path1).size());

    // use super user to set trusted xAttr
    fs.setXAttr(user1Path, "trusted.a1", attrValue2);

    assertEquals(1, user1fs.getXAttrs(path1).size());
    assertEquals(1, user1fs.listXAttrs(path1).size());
    assertEquals(2, fs.getXAttrs(user1Path).size());
    assertEquals(2, fs.listXAttrs(user1Path).size());
  }

  @Test
  public void testCanNotRemoveXAttrWithoutWPermission() throws Exception {
    createEmptyFile(user1fs, path1);
    user1fs.setXAttr(path1, attrName1, attrValue1);
    user1fs.setPermission(path1,
        FsPermission.createImmutable((short) 352));
    try {
      user1fs.removeXAttr(path1, attrName1);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }
    assertEquals(1, fs.listXAttrs(user1Path).size());
  }

  @Test
  public void testCanNotRemoveXAttrWithoutParentXPermission() throws Exception {
    createEmptyFile(user1fs, path1);
    user1fs.setXAttr(path1, attrName1, attrValue1);

    // remove parent node's X permission
    Path path1Parent = new Path("aaa/bbb");
    user1fs.setPermission(path1Parent,
        FsPermission.createImmutable((short) 384));
    try {
      user1fs.removeXAttr(path1, attrName1);
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }
    assertEquals(1, fs.listXAttrs(user1Path).size());
  }

  /**
   * Helper methods
   */
  private void createFiles() throws IOException {
    // file 1
    path1 = new Path("aaa/bbb/ccc");
    createEmptyFile(fs, path1);

    // file 2
    path2 = new Path("aaa/bbb/ddd");
    createEmptyFile(fs, path2);

    // non-existed file
    noThisPath = new Path("noThisFile");
  }

  private void createEmptyFile(FileSystem fs, Path path) throws IOException {
    final short permissionVal = 480; // 740
    FSDataOutputStream fsOutStream = fs.create(path,
                   new FsPermission(permissionVal), EnumSet.of(CREATE),
                                                 4096, (short)3, 512, null);
    fsOutStream.close();
  }

  private void initAttributes() {
    final int valueSize = 10;
    // attr1
    attrName1 = "user.attr1";    // there's naming rule
    attrValue1 = new byte[valueSize]; // randomly select a size
    new Random().nextBytes(attrValue1);
    // attr2
    attrName2 = "user.attr2";    // there's naming rule
    attrValue2 = new byte[valueSize]; // randomly select a size
    new Random().nextBytes(attrValue2);
    // attr3
    attrName3 = "user.attr3";    // there's naming rule
    attrValue3 = new byte[valueSize]; // randomly select a size
    new Random().nextBytes(attrValue3);
    // attr4
    attrName4 = "user.attr4";    // there's naming rule
    attrValue4 = new byte[valueSize]; // randomly select a size
    new Random().nextBytes(attrValue4);
  }

  private void setupForOtherUsers() throws IOException {
    user1Path = new Path("/user/user1/" + path1.toUri().getPath());
    Path user1Root = new Path ("/user/user1");
    fs.mkdirs(user1Root);
    fs.setOwner(user1Root, "user1", "mygroup");
  }

  static FileSystem getFS() throws IOException {
    try {
      return user1.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
