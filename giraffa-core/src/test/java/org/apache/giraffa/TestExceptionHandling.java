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
import org.apache.giraffa.hbase.ClientNamenodeProtocolServerSideCallbackTranslatorPB;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that Exceptions are properly transmitted from server to client. The
 * tests were selected based on the types of error serializations done in
 * {@link ClientNamenodeProtocolServerSideCallbackTranslatorPB}
 */
public class TestExceptionHandling {
  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseCommonTestingUtility.BASE_TEST_DIRECTORY_KEY,
        GiraffaTestUtils.BASE_TEST_DIRECTORY);
    UTIL.startMiniCluster(1);
    UTIL.setZkCluster(UTIL.getZkCluster());
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
    if(grfs != null) grfs.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test(expected = FileNotFoundException.class)
  public void testSetOwner() throws IOException {
    grfs.setOwner(new Path("file.txt"), "username", "groupname");
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testCreate() throws IOException {
    grfs.createNewFile(new Path("file.txt"));
    grfs.create(new Path("file.txt"), false);
  }

  @Test(expected = ParentNotDirectoryException.class)
  public void testMkdirs() throws IOException {
    grfs.createNewFile(new Path("file"));
    grfs.mkdirs(new Path("file/test"));
  }

  @Test(expected = IOException.class)
  public void testGetContentSummary() throws IOException {
    grfs.mkdirs(new Path("/path/to"));
    grfs.createNewFile(new Path("/path/to/file.txt"));
    grfs.grfaClient.getContentSummary("/path/to/file.txt");
  }

  @Test(expected = FileNotFoundException.class)
  public void testAddBlock() throws IOException {
    grfs.grfaClient.getNamenode().addBlock("/file.txt", "client", null, null,
        INodeId.GRANDFATHER_INODE_ID, null);
  }
}
