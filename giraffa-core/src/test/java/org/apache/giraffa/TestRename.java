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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.giraffa.RenameRecoveryState.PUT_SETFLAG;
import static org.apache.giraffa.RenameRecoveryState.PUT_NOFLAG;
import static org.apache.giraffa.RenameRecoveryState.DELETE;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.INodeManager;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRename {
  static final Log LOG = LogFactory.getLog(TestRename.class);
  private static final HBaseTestingUtility UTIL =
                                  GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;
  private Connection connection;
  private RowKeyFactory keyFactory;
  private INodeManager nodeManager;

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
    connection = ConnectionFactory.createConnection(conf);
    keyFactory = GiraffaTestUtils.createFactory(grfs);
    nodeManager = GiraffaTestUtils.getNodeManager(conf, connection, keyFactory);
  }

  @After
  public void after() throws IOException {
    IOUtils.cleanup(LOG, grfs, nodeManager, connection);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void createTestFile(String srcStr, char c) throws IOException {
    Path src = new Path(srcStr);
    FSDataOutputStream out = grfs.create(src, true, 5000, (short) 3, 512);
    for(int j = 0; j < 2000; j++) {
      out.write(c);
    }
    out.close();
  }

  private char readFile(Path src) throws IOException {
    FSDataInputStream input = grfs.open(src);
    try {
      int c = input.read();
      return (char) c;
    }finally {
      input.close();
    }
  }

  /**
   * Completes the given stage of the rename process.
   */
  private void doRenameStage(String src, String dst, RenameRecoveryState stage)
      throws IOException {
    src = new Path(grfs.getWorkingDirectory(), src).toUri().getPath();
    dst = new Path(grfs.getWorkingDirectory(), dst).toUri().getPath();

    if (stage == PUT_SETFLAG) {
      LOG.debug("Copying " + src + " to " + dst + " with rename flag");
      INode srcNode = nodeManager.getINode(src);
      RowKey dstKey = keyFactory.newInstance(dst, srcNode.getId());
      INode dstNode = srcNode.cloneWithNewRowKey(dstKey);
      dstNode.setRenameState(RenameState.TRUE(srcNode.getRowKey().getKey()));
      nodeManager.updateINode(dstNode, null, nodeManager.getXAttrs(src));
    }

    if (stage == DELETE) {
      INode srcNode = nodeManager.getINode(src);
      nodeManager.delete(srcNode);
    }

    if (stage == PUT_NOFLAG) {
      INode dstNode = nodeManager.getINode(dst);
      dstNode.setRenameState(RenameState.FALSE());
      nodeManager.updateINode(dstNode);
    }
  }

  private void renameFile(String src, String dst, boolean overwrite)
      throws IOException {
    Path srcPath = new Path(src);
    Path dstPath = new Path(dst);
    grfs.rename(srcPath, dstPath, overwrite ? Rename.OVERWRITE : Rename.NONE);
    assertTrue(grfs.exists(dstPath));
    assertFalse(grfs.exists(srcPath));
    assertTrue(readFile(dstPath) == 'A');
  }

  /**
   * Collects type and data information about the children of a directory.
   * @param path the directory whose children to analyze
   * @param isFile stores whether or not the node is a file for each child
   * @param firstChar stores the first character of each child that is a file
   */
  private void collectDirectoryChildrenInfo(Path path,
                                            Map<Path,Boolean> isFile,
                                            Map<Path,Character> firstChar)
      throws IOException {
    RemoteIterator<LocatedFileStatus> children = grfs.listFiles(path, true);
    while(children.hasNext()) {
      Path cur = new Path(children.next().getPath().toUri().getPath());
      // relative child paths will not change after renameFile
      Path rel = new Path(path.toUri().relativize(cur.toUri()));

      if(grfs.isFile(cur)) {
        isFile.put(rel, true);
        firstChar.put(rel, readFile(cur));
      }else {
        isFile.put(rel, false);
      }
    }
  }

  private void renameDir(String src, String dst, boolean overwrite)
      throws IOException {
    // collect information on children of src
    Map<Path,Boolean> isFile = new HashMap<Path,Boolean>();
    Map<Path,Character> firstChar = new HashMap<Path,Character>();
    collectDirectoryChildrenInfo(new Path(src), isFile, firstChar);
    renameDir(src, dst, overwrite, isFile, firstChar);
  }

  private void renameDir(String src, String dst, boolean overwrite,
                         Map<Path,Boolean> isFile1,
                         Map<Path,Character> firstChar1)
      throws IOException {
    Path srcPath = new Path(src);
    Path dstPath = new Path(dst);

    grfs.rename(srcPath, dstPath, overwrite ? Rename.OVERWRITE : Rename.NONE);

    // check that src has moved to dst
    assertTrue(grfs.exists(dstPath));
    assertFalse(grfs.exists(srcPath));

    // collect information on children of dst
    Map<Path,Boolean> isFile2 = new HashMap<Path,Boolean>();
    Map<Path,Character> firstChar2 = new HashMap<Path,Character>();
    collectDirectoryChildrenInfo(dstPath, isFile2, firstChar2);

    // check that information on src children was properly copied
    assertEquals(isFile1, isFile2);
    assertEquals(firstChar1, firstChar2);

    // check that src children no longer exist
    for(Path path : isFile1.keySet()) {
      assertFalse("Path "+path+" exists", grfs.exists(new Path(srcPath, path)));
    }
  }

  // ==== FILE RENAME: SUCCESS CASES ====

  @Test
  public void testFileRename() throws IOException {
    createTestFile("test", 'A');
    renameFile("test", "test2", false);
  }

  @Test
  public void testFileRenameRecoveryStage1Complete() throws IOException {
    grfs.mkdirs(new Path("dir"));
    createTestFile("test", 'A');
    doRenameStage("test", "dir/test", PUT_SETFLAG);
    renameFile("test", "dir/test", false);
  }

  @Test
  public void testFileRenameRecoveryStage2Complete() throws IOException {
    grfs.mkdirs(new Path("dir"));
    createTestFile("test", 'A');
    doRenameStage("test", "dir/test", PUT_SETFLAG);
    doRenameStage("test", "dir/test", DELETE);
    renameFile("test", "dir/test", false);
  }

  @Test
  public void testFileRenameOverwrite() throws IOException {
    createTestFile("test", 'A');
    createTestFile("test2", 'B');
    renameFile("test", "test2", true);
  }

  @Test
  public void testFileMove() throws IOException {
    grfs.mkdirs(new Path("dir"));
    createTestFile("test", 'A');
    renameFile("test", "dir/test", false);
  }

  @Test
  public void testFileMoveOverwrite() throws IOException {
    grfs.mkdirs(new Path("dir"));
    createTestFile("test", 'A');
    createTestFile("dir/test", 'B');
    renameFile("test", "dir/test", true);
  }

  // ==== FILE RENAME: FAIL CASES ===-=

  @Test(expected = FileNotFoundException.class)
  public void testFileRenameSrcMissingAndDstMissing() throws IOException {
    renameFile("test", "test2", false);
  }

  @Test(expected = FileNotFoundException.class)
  public void testFileRenameSrcMissingDstExists() throws IOException {
    createTestFile("test2", 'B');
    renameFile("test", "test2", false);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testFileRenameDstExistsNoOverwrite() throws IOException {
    createTestFile("test", 'A');
    createTestFile("test2", 'B');
    renameFile("test", "test2", false);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testFileRenameDstEqualsSrc() throws IOException {
    createTestFile("test", 'A');
    renameFile("test", "test", false);
  }

  @Test(expected = IOException.class)
  public void testFileMoveDstExistsAsDirectory() throws IOException {
    createTestFile("test", 'A');
    grfs.mkdirs(new Path("test2"));
    renameFile("test", "test2", true);
  }

  @Test(expected = FileNotFoundException.class)
  public void testFileMoveDstParentDoesNotExist() throws IOException {
    createTestFile("test", 'A');
    renameFile("test", "dir/test2", false);
  }

  @Test(expected = ParentNotDirectoryException.class)
  public void testFileMoveDstParentIsFile() throws IOException {
    createTestFile("test", 'A');
    createTestFile("file", 'C');
    renameFile("test", "file/test2", false);
  }

  // ==== DIRECTORY RENAME: SUCCESS CASES ====

  @Test
  public void testDirRename() throws IOException {
    grfs.mkdirs(new Path("/a/b/c"));
    createTestFile("/a/1", 't');
    createTestFile("/a/2", 'u');
    createTestFile("/a/b/1", 'v');
    createTestFile("/a/b/2", 'w');
    createTestFile("/a/b/c/1", 'x');
    createTestFile("/a/b/c/2", 'y');
    renameDir("/a", "/newA", false);
  }

  @Test
  public void testDirRenameOverwrite() throws IOException {
    grfs.mkdirs(new Path("/a/b/c"));
    createTestFile("/a/1", 't');
    createTestFile("/a/2", 'u');
    createTestFile("/a/b/1", 'v');
    createTestFile("/a/b/2", 'w');
    createTestFile("/a/b/c/1", 'x');
    createTestFile("/a/b/c/2", 'y');
    grfs.mkdirs(new Path("/newA")); // empty dst directory
    renameDir("/a", "/newA", true);
  }

  @Test
  public void testDirRenameNoChildren() throws IOException {
    grfs.mkdirs(new Path("/x/a"));
    renameDir("/x/a", "/x/newA", false);
  }

  @Test
  public void testDirRenameRecoveryStage1PartlyComplete() throws IOException {
    grfs.mkdirs(new Path("/a/b/c"));
    grfs.mkdirs(new Path("/dir"));
    createTestFile("/a/1", 't');
    createTestFile("/a/2", 'u');
    createTestFile("/a/b/1", 'v');
    createTestFile("/a/b/2", 'w');
    createTestFile("/a/b/c/1", 'x');
    createTestFile("/a/b/c/2", 'y');

    doRenameStage("/a/1", "/newA/1", PUT_SETFLAG);
    doRenameStage("/a/2", "/newA/2", PUT_SETFLAG);
    doRenameStage("/a/b", "/newA/b", PUT_SETFLAG);
    renameDir("/a", "/newA", false);
  }

  @Test
  public void testDirRenameRecoveryStage2PartlyComplete() throws IOException {
    grfs.mkdirs(new Path("/a/b/c"));
    grfs.mkdirs(new Path("/dir"));
    createTestFile("/a/1", 't');
    createTestFile("/a/2", 'u');
    createTestFile("/a/b/1", 'v');
    createTestFile("/a/b/2", 'w');
    createTestFile("/a/b/c/1", 'x');
    createTestFile("/a/b/c/2", 'y');

    // collect src information before doing partial rename
    Map<Path,Boolean> isFile = new HashMap<Path,Boolean>();
    Map<Path,Character> firstChar = new HashMap<Path,Character>();
    collectDirectoryChildrenInfo(new Path("/a"), isFile, firstChar);

    doRenameStage("/a", "/dir/a", PUT_SETFLAG);
    doRenameStage("/a/1", "/dir/a/1", PUT_SETFLAG);
    doRenameStage("/a/2", "/dir/a/2", PUT_SETFLAG);
    doRenameStage("/a/b", "/dir/a/b", PUT_SETFLAG);
    doRenameStage("/a/b/1", "/dir/a/b/1", PUT_SETFLAG);
    doRenameStage("/a/b/2", "/dir/a/b/2", PUT_SETFLAG);
    doRenameStage("/a/b/c", "/dir/a/b/c", PUT_SETFLAG);
    doRenameStage("/a/b/c/1", "/dir/a/b/c/1", PUT_SETFLAG);
    doRenameStage("/a/b/c/2", "/dir/a/b/c/2", PUT_SETFLAG);

    // deletes occur recursively from bottom to top
    doRenameStage("/a/b/c/2", "/dir/a/b/c/2", DELETE);
    doRenameStage("/a/b/c/1", "/dir/a/b/c/1", DELETE);
    doRenameStage("/a/b/c", "/dir/a/b/c", DELETE);
    doRenameStage("/a/b/2", "/dir/a/b/2", DELETE);
    renameDir("/a", "/dir/a", false, isFile, firstChar);
  }

  @Test
  public void testDirRenameRecoveryStage3PartlyComplete() throws IOException {
    grfs.mkdirs(new Path("/a/b/c"));
    grfs.mkdirs(new Path("/dir"));
    createTestFile("/a/1", 't');
    createTestFile("/a/2", 'u');
    createTestFile("/a/b/1", 'v');
    createTestFile("/a/b/2", 'w');
    createTestFile("/a/b/c/1", 'x');
    createTestFile("/a/b/c/2", 'y');

    // collect src information before doing partial rename
    Map<Path,Boolean> isFile = new HashMap<Path,Boolean>();
    Map<Path,Character> firstChar = new HashMap<Path,Character>();
    collectDirectoryChildrenInfo(new Path("/a"), isFile, firstChar);

    doRenameStage("/a", "/dir/a", PUT_SETFLAG);
    doRenameStage("/a/1", "/dir/a/1", PUT_SETFLAG);
    doRenameStage("/a/2", "/dir/a/2", PUT_SETFLAG);
    doRenameStage("/a/b", "/dir/a/b", PUT_SETFLAG);
    doRenameStage("/a/b/1", "/dir/a/b/1", PUT_SETFLAG);
    doRenameStage("/a/b/2", "/dir/a/b/2", PUT_SETFLAG);
    doRenameStage("/a/b/c", "/dir/a/b/c", PUT_SETFLAG);
    doRenameStage("/a/b/c/1", "/dir/a/b/c/1", PUT_SETFLAG);
    doRenameStage("/a/b/c/2", "/dir/a/b/c/2", PUT_SETFLAG);

    // deletes occur recursively from bottom to top
    doRenameStage("/a/b/c/2", "/dir/a/b/c/2", DELETE);
    doRenameStage("/a/b/c/1", "/dir/a/b/c/1", DELETE);
    doRenameStage("/a/b/c", "/dir/a/b/c", DELETE);
    doRenameStage("/a/b/2", "/dir/a/b/2", DELETE);
    doRenameStage("/a/b/1", "/dir/a/b/1", DELETE);
    doRenameStage("/a/b", "/dir/a/b", DELETE);
    doRenameStage("/a/2", "/dir/a/2", DELETE);
    doRenameStage("/a/1", "/dir/a/1", DELETE);
    doRenameStage("/a", "/dir/a", DELETE);

    doRenameStage("/a/1", "/dir/a/1", PUT_NOFLAG);
    doRenameStage("/a/2", "/dir/a/2", PUT_NOFLAG);
    doRenameStage("/a/b", "/dir/a/b", PUT_NOFLAG);
    renameDir("/a", "/dir/a", false, isFile, firstChar);
  }

  @Test
  public void testDirMoveDown() throws IOException {
    grfs.mkdirs(new Path("/a/b/c"));
    grfs.mkdirs(new Path("/d"));
    createTestFile("/a/1", 't');
    createTestFile("/a/2", 'u');
    createTestFile("/a/b/1", 'v');
    createTestFile("/a/b/2", 'w');
    createTestFile("/a/b/c/1", 'x');
    createTestFile("/a/b/c/2", 'y');
    renameDir("/a", "/d/newA", false);
  }

  @Test
  public void testDirMoveUp() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    renameDir("/x/y/a", "/x/newA", false);
  }

  @Test
  public void testDirMoveOverwrite() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    grfs.mkdirs(new Path("/x/newA")); // empty dst directory
    renameDir("/x/y/a", "/x/newA", true);
  }

  // ==== DIRECTORY RENAME: FAIL CASES ====

  @Test(expected = IOException.class)
  public void testDirRenameSrcIsRoot() throws IOException {
    renameDir("/", "test", false);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testDirRenameDstExistsNoOverwrite() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    grfs.mkdirs(new Path("/x/newA")); // empty dst directory
    renameDir("/x/y/a", "/x/newA", false);
  }

  @Test(expected = IOException.class)
  public void testDirRenameDstExistsAsFile() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    createTestFile("/x/newA", 'A'); // destination exists as file
    renameDir("/x/y/a", "/x/newA", true);
  }

  @Test(expected = IOException.class)
  public void testDirRenameDstNotEmpty() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');

    // create non-empty destination dir
    grfs.mkdirs(new Path("/x/newA"));
    createTestFile("/x/newA/test", 'A');
    renameDir("/x/y/a", "/x/newA", true);
  }

  @Test(expected = IOException.class)
  public void testDirRenameDstIsRoot() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    renameDir("/x/y/a", "/", true);
  }

  @Test(expected = FileNotFoundException.class)
  public void testDirRenameDstParentDoesNotExist() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    renameDir("/x/y/a", "/x/z/newA", false);
  }

  @Test(expected = ParentNotDirectoryException.class)
  public void testDirRenameDstParentIsFile() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    createTestFile("/x/z", 'A'); // destination parent exists as file
    renameDir("/x/y/a", "/x/z/newA", false);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testDirRenameDstEqualsSrc() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    renameDir("/x/y/a", "/x/y/a", true);
  }

  @Test(expected = IOException.class)
  public void testDirRenameDstIsInsideSrc() throws IOException {
    grfs.mkdirs(new Path("x/y/a/b/c"));
    createTestFile("/x/y/a/1", 't');
    createTestFile("/x/y/a/2", 'u');
    createTestFile("/x/y/a/b/1", 'v');
    createTestFile("/x/y/a/b/2", 'w');
    createTestFile("/x/y/a/b/c/1", 'x');
    createTestFile("/x/y/a/b/c/2", 'y');
    renameDir("/x/y/a", "/x/y/a/newA", false);
  }
}
