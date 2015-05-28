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

import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestBlockManagement {
  private static final HBaseTestingUtility UTIL =
                                  GiraffaTestUtils.getHBaseTestingUtility();
  private GiraffaFileSystem grfs;

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
    if(grfs != null) grfs.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testWriteAndDelete() throws IOException {
    Path file = new Path("writingA.txt");
    FSDataOutputStream out = grfs.create(file, true, 5000, (short) 3, 512);
    for(int i = 0; i < 2000; i++) {
      out.write('A');
    }
    out.close();

    FSDataInputStream in = grfs.open(file, 5000);
    for(int i = 0; i < 2000; i++) {
      assertEquals('A', in.readByte());
    }
    in.close();

    assertTrue(grfs.delete(file, false));
  }

  @Test
  public void testWriteRead() throws IOException {
    Path file = new Path("giraffa.txt");
    FSDataOutputStream out = grfs.create(file);
    for(int i = 0; i < 6000000; i++) {
      out.writeBytes("GIRAFFA");
    }
    out.close();

    FSDataInputStream in = grfs.open(file);
    for(int i = 0; i < 6000000; i++) {
      assertEquals('G', in.readByte());
      assertEquals('I', in.readByte());
      assertEquals('R', in.readByte());
      assertEquals('A', in.readByte());
      assertEquals('F', in.readByte());
      assertEquals('F', in.readByte());
      assertEquals('A', in.readByte());
    }
    try {
      in.readByte();
      fail();
    } catch (EOFException e) {
      //must catch
    }
    in.close();
  }

  public static void main(String[] args) throws Exception {
    TestBlockManagement test = new TestBlockManagement();
    GiraffaConfiguration conf =
      new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaTestUtils.setGiraffaURI(conf);
    test.grfs = (GiraffaFileSystem) FileSystem.get(conf);
    test.testWriteRead();
    test.after();
  }
}
