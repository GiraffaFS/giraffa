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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.giraffa.GiraffaTestUtils.printFileStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test common file system use cases.
 */
public class TestGiraffaFS {
  static final Log LOG = LogFactory.getLog(TestGiraffaFS.class);

  private static final HBaseTestingUtility UTIL =
    GiraffaTestUtils.getHBaseTestingUtility();
  private static GiraffaFileSystem grfs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, GiraffaTestUtils.BASE_TEST_DIRECTORY);
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

  @After
  public void after() throws IOException {
    IOUtils.cleanup(LOG, grfs);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFileCreation() throws IOException {
    grfs.create(new Path("text.txt"));
    grfs.create(new Path("plamen's test"));
    FileStatus[] files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(2, files.length);
    FileStatus stats[] = grfs.listStatus(new Path("/"));
    assertEquals(1, stats.length);
  }

  @Test
  public void testFileDeletion() throws IOException {
    grfs.create(new Path("text.txt"));
    grfs.create(new Path("plamen's test"));
    grfs.delete(new Path("plamen's test"), false);
    FileStatus[] files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(1, files.length);
  }

  @Test
  public void testDirCreation() throws IOException {
    grfs.mkdirs(new Path("folder1"));
    grfs.mkdirs(new Path("folder2"));
    FileStatus[] files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(2, files.length);
  }

  @Test
  public void testDuplicateFileAndDir() throws IOException {
    grfs.mkdirs(new Path("folder1"));
    grfs.mkdirs(new Path("folder1/f2"));
    grfs.create(new Path("file.txt"));
    grfs.create(new Path("folder1/f2/file.txt"));

    assertTrue("mkdirs() should not fail for existing directories",
        grfs.mkdirs(new Path("folder1")));
    try {
      grfs.create(new Path("file.txt"), false);
      fail();
    } catch (FileAlreadyExistsException e) {
      e.getLocalizedMessage();//must catch
    } catch (Throwable t) {
      t.getLocalizedMessage();
    }

    assertTrue("mkdirs() should not fail for existing directories",
      grfs.mkdirs(new Path("folder1/f2")));

    try {
      grfs.create(new Path("folder1/f2/file.txt"), true);
    } catch (FileAlreadyExistsException e) {
      fail();
    }
    FileStatus[] files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(2, files.length);
  }

  @Test
  public void testMassDirCreationAndDeletion() throws IOException {
    String path = "./";
    for(int i = 0; i < 30; i++) {
      path += (i + "/");
    }
    assertTrue(grfs.mkdirs(new Path(path)));
    FileStatus[] files =
      GiraffaTestUtils.listStatusRecursive(grfs, new Path("."));
    
    assertEquals(30, files.length);
    for(int i = 0; i < 30; i++) {
      assertEquals(String.valueOf(i), files[i].getPath().getName());
    }

    assertTrue(grfs.delete(new Path("0"), true));

    files = grfs.listStatus(new Path("."));
    assertEquals(0, files.length);
  }

  @Test
  public void testDeletionNonRecursiveNoChildren() throws IOException {
    grfs.mkdirs(new Path("folder"));
    assertTrue(grfs.delete(new Path("folder"), false));
    FileStatus[] files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(0, files.length);
  }

  @Test
  public void testDeletionNonRecursiveWithChildren() throws IOException {
    grfs.mkdirs(new Path("folder1/folder3"));
    grfs.mkdirs(new Path("folder2"));
    grfs.create(new Path("folder2/file1")).close();
    assertFalse(grfs.delete(new Path("folder1"), false));
    assertFalse(grfs.delete(new Path("folder2"), false));
    FileStatus[] files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(2, files.length);
  }

  @Test
  public void testDeletionRecursive() throws IOException {
    grfs.mkdirs(new Path("folder1"));
    grfs.mkdirs(new Path("folder2"));
    grfs.create(new Path("folder2/test.txt"));
    grfs.mkdirs(new Path("folder1/folder3"));
    grfs.create(new Path("folder1/folder3/test.txt"));
    FileStatus[] files = grfs.listStatus(new Path("."));
    assertEquals(2, files.length);
    grfs.delete(new Path("folder1"), true);
    files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(1, files.length);
  }

  @Test
  public void testDirAttributes() throws IOException {
    grfs.mkdirs(new Path("folder2"));

    LOG.debug("SETTING PERMISSION OF \"folder2\" TO 755");
    grfs.setPermission(new Path("folder2"), new FsPermission((short) 0755));

    LOG.debug("SETTING TIMES OF \"folder2\" TO M:25, A:30");
    grfs.setTimes(new Path("folder2"), 25, 30);

    FileStatus[] files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(1, files.length);
    assertFalse(0 == files[0].getAccessTime());
    assertFalse(0 == files[0].getModificationTime());
    assertEquals(0755, files[0].getPermission().toShort());
  }

  @Test
  public void testFileAttributes() throws IOException {
    grfs.create(new Path("file2"));

    LOG.debug("SETTING PERMISSION OF \"file2\" TO 755");
    grfs.setPermission(new Path("file2"), new FsPermission((short) 0755));

    LOG.debug("SETTING TIMES OF \"file2\" TO M:25, A:30");
    grfs.setTimes(new Path("file2"), 25, 30);

    FileStatus[] files = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(1, files.length);
    assertEquals(30, files[0].getAccessTime());
    assertEquals(25, files[0].getModificationTime());
    assertEquals(0644, files[0].getPermission().toShort());
  }

  @Test
  public void testSimpleGetFileInfo() throws IOException {
    grfs.mkdirs(new Path("folder2"));
    grfs.create(new Path("folder2/folder2"));
    FileStatus fileStat = grfs.getFileStatus(new Path("folder2"));
    printFileStatus(fileStat);
    assertEquals("folder2", fileStat.getPath().getName());
    fileStat = grfs.getFileStatus(new Path("folder2/folder2"));
    printFileStatus(fileStat);
    assertEquals("folder2", fileStat.getPath().getName());
  }

  public static void main(String[] args) throws Exception {
    TestGiraffaFS test = new TestGiraffaFS();
    GiraffaConfiguration conf =
      new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaFileSystem.format(conf, true);
    GiraffaTestUtils.setGiraffaURI(conf);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
    test.testFileCreation();
  }
}
