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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hdfs.GiraffaClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestGiraffaFileStatus {
  private static MiniHBaseCluster cluster;
  private static final HBaseTestingUtility UTIL =
                                  GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;
  private GiraffaClient grfaClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, GiraffaTestUtils.BASE_TEST_DIRECTORY);
    cluster = UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws IOException {
    GiraffaConfiguration conf =
      new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
    grfaClient = grfs.grfaClient;
  }

  @After
  public void after() throws IOException {
    if(grfs != null) grfs.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testCompletedLocatedFileStatus() throws IOException {
    Path file = new Path("/fileA");
    FSDataOutputStream out = grfs.create(file, true, 5000, (short) 3, 512);
    for(int i = 0; i < 9876; i++) {
      out.write('A');
    }
    out.close();

    DirectoryListing listing = grfaClient.listPaths("/fileA", null, true);
      assertTrue("DirectoryListing.getPartialListing() returned empty result.",
              listing.getPartialListing().length > 0);
    HdfsFileStatus status = listing.getPartialListing()[0];

    assertTrue("Returned FileStatus was not HdfsLocatedFileStatus.", 
        status instanceof HdfsLocatedFileStatus);
    
    LocatedBlocks blocks = ((HdfsLocatedFileStatus) status).getBlockLocations();
    assertEquals("Incorrect number of blocks returned.", (int)Math.ceil(9876/512d), blocks.getLocatedBlocks().size());
    assertFalse("Marked under construction.", blocks.isUnderConstruction());
    assertTrue("Last block not complete.", blocks.isLastBlockComplete());

    assertTrue("File size by blocks differs from status.", blocks.getFileLength() == status.getLen());

    assertTrue(grfs.delete(file, false));
  }

  @Test
  public void testNotLocatedFileStatus() throws IOException {
    Path file = new Path("/fileM");
    FSDataOutputStream out = grfs.create(file, true, 5000, (short) 3, 512);
    for(int i = 0; i < 12345; i++) {
      out.write('M');
    }
    out.close();

    DirectoryListing listing = grfaClient.listPaths("/fileM", null, false);
    assertTrue("DirectoryListing.getPartialListing() returned empty result.",
            listing.getPartialListing().length > 0);
    HdfsFileStatus status = listing.getPartialListing()[0];

    assertTrue("Returned FileStatus was not HdfsFileStatus.", status instanceof HdfsFileStatus);

    assertTrue(grfs.delete(file, false));
  }

  //TODO: fix the tests, see http://code.google.com/a/apache-extras.org/p/giraffa/issues/detail?id=61 for details
  //@Test
  public void testUnderConstructionLocatedFileStatus() throws IOException, InterruptedException {
    Path file = new Path("/fileB");
    FSDataOutputStream out = grfs.create(file, true, 5000, (short) 3, 512);
    for(int i = 0; i < 12345; i++) {
      out.write('B');
    }

    DirectoryListing listing = grfaClient.listPaths("/fileB", null, true);
    assertTrue("DirectoryListing.getPartialListing() returned empty result.",
              listing.getPartialListing().length > 0);
    HdfsFileStatus status = listing.getPartialListing()[0];

    assertTrue("Returned FileStatus was not HdfsLocatedFileStatus.", 
        status instanceof HdfsLocatedFileStatus);
    
    LocatedBlocks blocks = ((HdfsLocatedFileStatus) status).getBlockLocations();
    assertEquals("Incorrect number of blocks returned.", (int) Math.ceil(12345/512d), blocks.getLocatedBlocks().size());
    assertTrue("Not marked under construction.", blocks.isUnderConstruction());
    assertFalse("Last block marked complete.", blocks.isLastBlockComplete());

    assertTrue("File size by blocks differs from status.", blocks.getFileLength() == status.getLen());

    assertTrue(grfs.delete(file, false));
  }

  public static void main(String[] args) throws IOException {
    TestGiraffaFileStatus test = new TestGiraffaFileStatus();
    GiraffaConfiguration conf =
      new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    test.grfs = (GiraffaFileSystem) FileSystem.get(conf);
    test.testCompletedLocatedFileStatus();
    test.after();
  }
}
