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

import mockit.Deencapsulation;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.TestDFSPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;


public class TestGiraffaPermission extends TestDFSPermission {

  private static MockTestDFSPermission mockTest;
  private static HBaseTestingUtility UTIL;
  private static GiraffaConfiguration conf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY,
        GiraffaTestUtils.BASE_TEST_DIRECTORY);
    mockTest = new MockTestDFSPermission();
    UTIL = GiraffaTestUtils.getHBaseTestingUtility();
    UTIL.startMiniCluster();
    conf = new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    Deencapsulation.setField(TestDFSPermission.class, conf);
  }

  @Before
  @Override
  public void setUp() throws IOException {
    GiraffaFileSystem.format(conf, false);
  }

  @After
  @Override
  public void tearDown() throws IOException {
    // do nothing
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
    mockTest.tearDown();
  }

  /**
   * Prevents ancestor permission checking.
   */
  private static class MockTestDFSPermission extends MockUp<TestDFSPermission> {
    @Mock
    public void testPermissionCheckingPerUser(Invocation inv,
                                              UserGroupInformation ugi,
                                              short[] ancestorPermission,
                                              short[] parentPermission,
                                              short[] filePermission,
                                              Path[] parentDirs,
                                              Path[] files,
                                              Path[] dirs) {
      for (int i = 0; i < ancestorPermission.length; i++) {
        ancestorPermission[i] = 0777;
      }
      inv.proceed();
    }
  }
}
