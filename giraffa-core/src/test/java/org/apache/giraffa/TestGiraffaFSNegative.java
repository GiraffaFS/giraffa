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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.giraffa.GiraffaTestUtils.printFileStatus;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests negative use cases and failure scenarios.
 */
public class TestGiraffaFSNegative {
  static final Log LOG = LogFactory.getLog(TestGiraffaFSNegative.class);

  private static final HBaseTestingUtility UTIL =
    GiraffaTestUtils.getHBaseTestingUtility();
  private static GiraffaFileSystem grfs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, GiraffaTestUtils.BASE_TEST_DIRECTORY);
    Configuration hbaseConf = UTIL.getConfiguration();
    hbaseConf.setBoolean(DFS_NAMENODE_XATTRS_ENABLED_KEY, false);
    UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws IOException {
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IOUtils.cleanup(LOG, grfs);
    UTIL.shutdownMiniCluster();
  }


  @Test
  public void testFileCreationNoParentDir() throws IOException {
    try {
      grfs.create(new Path("folder1/text.txt"));
    } catch (FileNotFoundException e) {
      fail();
    }
    grfs.create(new Path("plamen's test"));
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(3, files.length);
  }

  @Test
  public void testFileCreationAfterRecursiveDeletion() throws IOException {
    grfs.mkdirs(new Path("folder1"));
    grfs.mkdirs(new Path("folder1/folder2"));
    grfs.create(new Path("folder1/folder2/file3.file"));
    grfs.create(new Path("folder1/text.txt"));
    
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(4, files.length);

    assertTrue(grfs.delete(new Path("folder1"), true));
    
    files = GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(0, files.length);
    
    grfs.create(new Path("folder1/text.txt"));

    files = GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(2, files.length);
  }

  @Test
  public void testFileDeletionFromWrongSrcPath() throws IOException {
    grfs.create(new Path("text.txt"));
    try {
      boolean deleted = grfs.delete(new Path("folder1/text.txt"), true);
      assertFalse("Non existing file cannot be actually deleted.", deleted);
    } catch (FileNotFoundException e) {
      //succeed
    }
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(1, files.length);
  }

  @Test
  public void testRootDeletion() throws IOException {
    try {
      grfs.delete(new Path("/"), false);
      fail();
    } catch (FileNotFoundException e) {
      //succeed
    }
    try {
      grfs.delete(new Path("/"), true);
      fail();
    } catch (FileNotFoundException e) {
      //succeed
    }
    FileStatus[] files = grfs.listStatus(new Path("/"));
    printFileStatus(files);
    assertEquals(1, files.length);
  }

  @Test
  public void testRootOverwrite() throws IOException {
    try {
      grfs.create(new Path("/"), false);
      fail();
    } catch (IOException e) {
      //must catch to succeed
    }

    FileStatus[] files = grfs.listStatus(new Path("/"));
    printFileStatus(files);
    assertEquals(1, files.length);
  }

  @Test
  public void testRootGeneration() throws IOException {
    grfs.create(new Path("derp"), true);
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("/"));
    printFileStatus(files);
    assertEquals(3, files.length);
  }

  @Test
  public void testDirCreationWithBadParentDir() throws IOException {
    grfs.mkdirs(new Path("folder1"));
    try {
      grfs.mkdirs(new Path("folder1/folder2/folder1"));
    } catch (FileNotFoundException e) {
      fail();
    }
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(3, files.length);
  }

  @Test
  public void testDirCreationFoldersWithSameName() throws IOException {
    grfs.mkdirs(new Path("folder1"));
    grfs.mkdirs(new Path("folder1/folder2/"));
    grfs.mkdirs(new Path("folder1/folder2/folder1"));
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(3, files.length);
  }

  @Test
  public void testDirDeletionWithRecurse() throws IOException {
    grfs.mkdirs(new Path("folder1"));
    grfs.mkdirs(new Path("folder1/folder2/"));
    grfs.mkdirs(new Path("folder1/folder2/folder1"));
    FSDataOutputStream out =
      grfs.create(new Path("folder1/folder2/folder1/amazing.txt"));
    for(int i = 0; i < 35000000; i++) {
      out.write('Q');
    }
    out.close();
    grfs.delete(new Path("folder1/folder2/folder1"), true);
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(2, files.length);
  }

  @Test
  public void testDirDeletionWithRecurseAndThenTryToCreate() throws IOException {
    grfs.mkdirs(new Path("folder1"));
    grfs.mkdirs(new Path("folder1/folder2/"));
    grfs.create(new Path("folder1/letext.txt"));
    grfs.mkdirs(new Path("folder1/folder2/folder1"));
    grfs.mkdirs(new Path("folder1/folder2/folder1/f3"));
    grfs.mkdirs(new Path("folder1/folder2/folder1/f4"));
    grfs.mkdirs(new Path("folder1/folder2/folder1/f6"));
    grfs.create(new Path("folder1/folder2/folder1/amazing.txt"));
    grfs.delete(new Path("folder1"), true);
    try {
      grfs.create(new Path("folder1/folder2/nopsled.blob"));
    } catch (FileNotFoundException e) {
      fail();
    }
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(3, files.length);
  }

  @Test
  // FileNotFoundException does not work now.
  // setTime(), setPermissions() don't throw since HBase RPC
  // treats all exceptions as errors, and does not pass the legal ones 
  // back to the client.
  public void testDirSettingsOnNonExistentFolder() throws IOException {
    grfs.mkdirs(new Path("folder1/folder2"));

    LOG.debug("SETTING PERMISSION OF \"folder2\" TO 777");
    try {
      grfs.setPermission(new Path("folder2"), new FsPermission((short) 0777));
      fail();
    } catch (FileNotFoundException e) {
      //must catch
    }

    LOG.debug("SETTING TIMES OF \"folder2\" TO M:25, A:30");
    try {
      grfs.setTimes(new Path("folder2"), 25, 30);
      fail();
    } catch (FileNotFoundException e) {
      //must catch
    }

    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    printFileStatus(files);
    assertEquals(2, files.length);
  }

  @Test
  public void testGetFileInfoOnNonExistentFile() throws IOException {
    grfs.mkdirs(new Path("folder2"));
    grfs.create(new Path("folder2/folder2"));
    try {
      grfs.getFileStatus(new Path("folder3"));
      fail("folder3 should not exist");
    } catch (FileNotFoundException e) {
      //must catch
    }
    try {
      grfs.getFileStatus(new Path("folder2/folder3"));
      fail("folder2/folder3 should not exist");
    } catch (FileNotFoundException e) {
      //must catch
    }
  }

  @Test
  public void testCanNotSetXAttrWhenFlagIsDisable() throws IOException {
    Path path = new Path("abcd");
    String attrName = "user.attr1";    // there's naming rule
    byte[] attrValue = new byte[20];   // randomly select a size

    try {
      grfs.setXAttr(path, attrName, attrValue);
      fail("Should not be able to set xAttr");
    } catch (IOException e) {
      assertTrue(e.toString().contains(DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }

    try {
      grfs.setXAttr(path, attrName, attrValue,
                    EnumSet.of(XAttrSetFlag.CREATE));
      fail("Should not be able to set xAttr");
    } catch (IOException e) {
      assertTrue(e.toString().contains(DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }
  }

  @Test
  public void testCanNotListXAttrWhenFlagIsDisable() throws IOException {
    try {
      grfs.listXAttrs(new Path("abcd"));
      fail("Should not be able to list xAttr");
    } catch (IOException e) {
      assertTrue(e.toString().contains(DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }
  }

  @Test
  public void testCanNotGetXAttrWhenFlagIsDisable() throws IOException {
    Path path = new Path("abcd");
    String attrName = "user.attr1";    // there's naming rule

    try {
      grfs.getXAttr(path, attrName);
      fail("Should not be able to get xAttr");
    } catch (IOException e) {
      assertTrue(e.toString().contains(DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }

    try {
      grfs.getXAttrs(path);
      fail("Should not be able to get xAttr");
    } catch (IOException e) {
      assertTrue(e.toString().contains(DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }

    try {
      grfs.getXAttrs(path, Collections.singletonList(attrName));
      fail("Should not be able to get xAttr");
    } catch (IOException e) {
      assertTrue(e.toString().contains(DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }
  }

  @Test
  public void testCanNotRemoveXAttrWhenFlagIsDisable() throws IOException {
    Path path = new Path("abcd");
    String attrName = "user.attr1";    // there's naming rule
    try {
      grfs.removeXAttr(path, attrName);
      fail("Should not be able to remove xAttr");
    } catch (IOException e) {
      assertTrue(e.toString().contains(DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }
  }

  public static void main(String[] args) throws Exception {
    TestGiraffaFSNegative test = new TestGiraffaFSNegative();
    GiraffaConfiguration conf =
      new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaFileSystem.format(conf, true);
    GiraffaTestUtils.setGiraffaURI(conf);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
    test.testRootDeletion();
  }
}
