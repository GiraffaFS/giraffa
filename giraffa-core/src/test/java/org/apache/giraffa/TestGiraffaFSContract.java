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

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.hbase.HBaseTestingUtility;

/**
 * Test filesystem contract
 */
public class TestGiraffaFSContract extends FileSystemContractBaseTest {

  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();

  public TestGiraffaFSContract(String name) {
    setName(name);
  }

  @Override
  public void setUp() throws Exception {
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    fs = FileSystem.get(conf);
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    fs.close();
  }

  @Override
  public void testWorkingDirectory() throws Exception {
    // TODO: fix test
  }

  @Override
  public void testMkdirsWithUmask() throws Exception {
    // TODO: fix test
  }

  @Override
  public void testOverwrite() throws IOException {
    // TODO: fix test
  }

  @Override
  public void testOverWriteAndRead() throws Exception {
    // TODO: fix test
  }

  @Override
  public void testDeleteRecursively() throws IOException {
    // TODO: fix test
  }

  @Override
  public void testRenameFileAsExistingDirectory() throws Exception {
    // TODO: fix test
  }

  @Override
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    // TODO: fix test
  }

  public static Test suite() throws Exception {
    TestSuite suite = new TestSuite(TestGiraffaFSContract.class);
    return new TestSetup(suite) {
      @Override
      protected void setUp() throws Exception {
        System.setProperty(
            HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY,
            GiraffaTestUtils.BASE_TEST_DIRECTORY);
        UTIL.startMiniCluster(1);
      }

      @Override
      protected void tearDown() throws Exception {
        UTIL.shutdownMiniCluster();
      }
    };
  }
}
