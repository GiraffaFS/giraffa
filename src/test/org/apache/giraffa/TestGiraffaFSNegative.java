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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests negative use cases and failure scenarios.
 */
public class TestGiraffaFSNegative {
  private static final String BASE_TEST_DIRECTORY = "build/test-data";
  private static final HBaseTestingUtility UTIL =
    GiraffaTestUtils.getHBaseTestingUtility();
  GiraffaFileSystem grfs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, BASE_TEST_DIRECTORY);
    UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws Exception {
    GiraffaConfiguration conf =
      new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
  }

  @After
  public void after() throws Exception {
    grfs.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
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
      grfs.delete(new Path("folder1/text.txt"), true);
      fail();
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

    try {
      //can't create on top of root
      grfs.mkdirs(new Path("/"));
      fail();
    } catch (IOException e) {
      assertEquals(e.getMessage(), "Root has no parent.");
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
  public void testDirSettingsOnNonExistentFolder() throws IOException {
    grfs.mkdirs(new Path("folder1/folder2"));

    System.out.println("SETTING PERMISSION OF \"folder2\" TO 777");
    try {
      grfs.setPermission(new Path("folder2"), new FsPermission((short) 777));
      fail();
    } catch (FileNotFoundException e) {
      //must catch
    }

    System.out.println("SETTING TIMES OF \"folder2\" TO M:25, A:30");
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

  private void printFileStatus(FileStatus[] fileStat) throws IOException {
    for (int i = 0; i < fileStat.length; i++) {
      System.out.println("===============FILE STATUS "+(i+1)+"===============");
      System.out.println("OWNER: " + fileStat[i].getOwner());
      System.out.println("GROUP: " + fileStat[i].getGroup());
      System.out.println("PATH: " + fileStat[i].getPath());
      System.out.println("PERMS: " + fileStat[i].getPermission().toString());
      System.out.println("LEN: " + fileStat[i].getLen());
      System.out.println("ATIME: " + fileStat[i].getAccessTime());
      System.out.println("MTIME: " + fileStat[i].getModificationTime());
      System.out.println("BLKSIZE: " + fileStat[i].getBlockSize());
      System.out.println("REPL: " + fileStat[i].getReplication());
      System.out.println("SYMLINK: " + fileStat[i].getSymlink());
    }
  }

  public static void main(String[] args) throws Exception {
    TestGiraffaFSNegative test = new TestGiraffaFSNegative();
    GiraffaConfiguration conf =
      new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaFileSystem.format(conf, true);
    GiraffaTestUtils.setGiraffaURI(conf);
    test.grfs = (GiraffaFileSystem) FileSystem.get(conf);
    test.testRootDeletion();
    test.after();
  }
}
