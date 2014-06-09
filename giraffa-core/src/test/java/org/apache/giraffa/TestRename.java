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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.giraffa.hbase.NamespaceProcessor.RenameRecoveryState.*;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.giraffa.hbase.NamespaceAgent.BlockAction;
import org.apache.giraffa.hbase.NamespaceProcessor;
import org.apache.giraffa.hbase.NamespaceProcessor.RenameRecoveryState;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRename {
  private static MiniHBaseCluster cluster;
  private static final HBaseTestingUtility UTIL =
                                  GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;
  private HTable table;

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
    table = new HTable(cluster.getConfiguration(),
        GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT.getBytes());
  }

  @After
  public void after() throws IOException {
    if(grfs != null) grfs.close();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    cluster.shutdown();
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

  private void rename(String srcStr, String dstStr, RenameRecoveryState stage)
      throws IOException {
    Path src = new Path(grfs.getWorkingDirectory(), srcStr);
    Path dst = new Path(grfs.getWorkingDirectory(), dstStr);

    // initialize namespace table based on given stage
    if(stage == PUT_SETFLAG || stage == DELETE) {
      createTestFile(src.toString(), 'A');
    }
    if(stage == DELETE || stage == PUT_NOFLAG) {
      RowKey srcKey = new FullPathRowKey(src.toString());
      RowKey dstKey = new FullPathRowKey(dst.toString());

      INode dstNode;
      if(stage == DELETE) {
        INode srcNode = newINode(src.toString(),
            table.get(new Get(srcKey.getKey())), true);
        dstNode = srcNode.cloneWithNewRowKey(dstKey);
      }
      else {
        createTestFile(dst.toString(), 'A');
        dstNode = newINode(dst.toString(),
            table.get(new Get(dstKey.getKey())), true);
      }

      dstNode.setRenameState(RenameState.TRUE(srcKey.getKey()));
      updateINode(dstNode, null);
    }

    // rename and validate changes
    grfs.rename(src, dst, Rename.NONE);
    assertTrue(grfs.exists(dst));
    assertFalse(grfs.exists(src));
    assertTrue(readFile(dst) == 'A');
  }

  @SuppressWarnings("unused") //for use when directory renames are implemented
  private void renameDir(String srcStr, String dstStr) throws IOException {
    Path src = new Path(srcStr);
    Path dst = new Path(dstStr);

    // these two hash maps store information about current state of src
    Map<URI,Boolean> isFile = new HashMap<URI,Boolean>();
    Map<URI,Character> firstChar = new HashMap<URI,Character>();

    // gather information about current subfiles
    RemoteIterator<LocatedFileStatus> subFiles = grfs.listFiles(src, true);
    while(subFiles.hasNext()) {
      Path cur = subFiles.next().getPath();
      URI curUri = cur.toUri().relativize(src.toUri());

      if(grfs.isFile(cur)) {
        isFile.put(curUri, true);
        firstChar.put(curUri, readFile(cur));
      }else {
        isFile.put(curUri, false);
      }
    }

    assertTrue(grfs.rename(src, dst)); // do rename

    // validate changes
    assertFalse(grfs.exists(src));
    assertTrue(grfs.exists(dst));

    // validate information about renamed subfiles
    subFiles = grfs.listFiles(dst, true);
    while(subFiles.hasNext()) {
      Path cur = subFiles.next().getPath();
      URI curUri = cur.toUri().relativize(src.toUri());

      // check that current file previously existed in src and is of same type
      assertTrue(isFile.get(curUri) != null);
      boolean shouldBeFile = isFile.get(curUri);
      assertTrue(shouldBeFile == grfs.isFile(cur));

      if(shouldBeFile) { // check that content of current file is unchanged
        assertTrue(readFile(cur) == firstChar.get(curUri).charValue());
      }
    }

    // check that all original subfiles exist under dst
    URI[] uris = (URI[]) isFile.keySet().toArray();
    for(URI uri : uris) {
      Path cur = new Path(dst, new Path(uri));
      assertTrue(grfs.exists(cur));
    }
  }

  @Test
  public void testFileInPlaceStage1() throws IOException {
    rename("test.txt", "test2.txt", PUT_SETFLAG);
  }

  @Test
  public void testFileInPlaceStage2() throws IOException {
    grfs.mkdirs(new Path("dir"));
    rename("dir/test.txt", "dir/test2.txt", DELETE);
  }

  @Test
  public void testFileInPlaceStage3() throws IOException {
    grfs.mkdirs(new Path("/dir"));
    rename("/dir/test.txt", "/dir/test2.txt", PUT_NOFLAG);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testFileInPlaceOverwrite() throws IOException {
    createTestFile("test2.txt", 'B');
    rename("test.txt", "test2.txt", PUT_SETFLAG);
  }

  @Test(expected = IOException.class)
  public void testFileInPlaceDestinationIsDirectory() throws IOException {
    grfs.mkdirs(new Path("dir"));
    rename("test.txt", "dir", PUT_SETFLAG);
  }

  @Test
  public void testFileMoveStage1() throws IOException {
    grfs.mkdirs(new Path("dir"));
    rename("test.txt", "dir/test.txt", PUT_SETFLAG);
  }

  @Test
  public void testFileMoveStage2() throws IOException {
    grfs.mkdirs(new Path("dir"));
    rename("dir/test.txt", "test2.txt", DELETE);
  }

  @Test
  public void testFileMoveStage3() throws IOException {
    grfs.mkdirs(new Path("dir"));
    grfs.mkdirs(new Path("dir2"));
    rename("dir/test.txt", "dir2/test.txt", PUT_NOFLAG);
  }

  @Test(expected = IOException.class)
  public void testFileMoveInvalidDestination() throws IOException {
    rename("test.txt", "dir/test.txt", PUT_SETFLAG);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testFileMoveOverwrite() throws IOException {
    grfs.mkdirs(new Path("dir"));
    createTestFile("dir/test.txt", 'B');
    rename("test.txt", "dir/test.txt", PUT_SETFLAG);
  }

  private INode newINode(String src, Result result, boolean needLocation)
      throws IOException {
    RowKey key = new FullPathRowKey(src);
    INode iNode = new INode(
        NamespaceProcessor.getLength(result),
        NamespaceProcessor.getDirectory(result),
        NamespaceProcessor.getReplication(result),
        NamespaceProcessor.getBlockSize(result),
        NamespaceProcessor.getMTime(result),
        NamespaceProcessor.getATime(result),
        NamespaceProcessor.getPermissions(result),
        NamespaceProcessor.getUserName(result),
        NamespaceProcessor.getGroupName(result),
        NamespaceProcessor.getSymlink(result),
        key,
        NamespaceProcessor.getDsQuota(result),
        NamespaceProcessor.getNsQuota(result),
        NamespaceProcessor.getFileState(result),
        NamespaceProcessor.getRenameState(result),
        (needLocation) ? NamespaceProcessor.getBlocks(result) : null,
        (needLocation) ? NamespaceProcessor.getLocations(result) : null);
    return iNode;
  }

  private void updateINode(INode node, BlockAction ba) throws IOException {
    long ts = Time.now();
    RowKey key = node.getRowKey();
    Put put = new Put(key.getKey(), ts);
    put.add(FileField.getFileAttributes(), FileField.getFileName(), ts,
            RowKeyBytes.toBytes(new Path(key.getPath()).getName()))
        .add(FileField.getFileAttributes(), FileField.getUserName(), ts,
            RowKeyBytes.toBytes(node.getOwner()))
        .add(FileField.getFileAttributes(), FileField.getGroupName(), ts,
            RowKeyBytes.toBytes(node.getGroup()))
        .add(FileField.getFileAttributes(), FileField.getLength(), ts,
            Bytes.toBytes(node.getLen()))
        .add(FileField.getFileAttributes(), FileField.getPermissions(), ts,
            Bytes.toBytes(node.getPermission().toShort()))
        .add(FileField.getFileAttributes(), FileField.getMTime(), ts,
            Bytes.toBytes(node.getModificationTime()))
        .add(FileField.getFileAttributes(), FileField.getATime(), ts,
            Bytes.toBytes(node.getAccessTime()))
        .add(FileField.getFileAttributes(), FileField.getDsQuota(), ts,
            Bytes.toBytes(node.getDsQuota()))
        .add(FileField.getFileAttributes(), FileField.getNsQuota(), ts,
            Bytes.toBytes(node.getNsQuota()))
        .add(FileField.getFileAttributes(), FileField.getReplication(), ts,
            Bytes.toBytes(node.getReplication()))
        .add(FileField.getFileAttributes(), FileField.getBlockSize(), ts,
            Bytes.toBytes(node.getBlockSize()))
        .add(FileField.getFileAttributes(), FileField.getRenameState(), ts,
            node.getRenameStateBytes());

    if(node.getSymlink() != null)
      put.add(FileField.getFileAttributes(), FileField.getSymlink(), ts,
          node.getSymlink());

    if(node.isDir())
      put.add(FileField.getFileAttributes(), FileField.getDirectory(), ts,
          Bytes.toBytes(node.isDir()));
    else
      put.add(FileField.getFileAttributes(), FileField.getBlock(), ts,
             node.getBlocksBytes())
         .add(FileField.getFileAttributes(), FileField.getLocations(), ts,
             node.getLocationsBytes())
         .add(FileField.getFileAttributes(), FileField.getFileState(), ts,
             Bytes.toBytes(node.getFileState().toString()));

    if(ba != null)
      put.add(FileField.getFileAttributes(), FileField.getAction(), ts,
          Bytes.toBytes(ba.toString()));

    synchronized(table) {
      table.put(put);
    }
  }
}
