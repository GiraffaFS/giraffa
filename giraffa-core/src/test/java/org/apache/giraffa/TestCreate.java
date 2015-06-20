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
import java.io.FileNotFoundException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.giraffa.GiraffaTestUtils.printFileStatus;
import static org.apache.hadoop.fs.CreateFlag.APPEND;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * This file is dedicated to test create methods of GiraffaFileSystem.
 * For now it focus on testing different flag combinations.
 * All the before/after derived from TestGiraffaFS.java
 */
public class TestCreate {
  static final Log LOG = LogFactory.getLog(TestCreate.class);

  private static final HBaseTestingUtility UTIL =
    GiraffaTestUtils.getHBaseTestingUtility();
  private static GiraffaFileSystem grfs;
  private Path path;
  private FsPermission permission;
  private int bufferSize;
  private short replication;
  private long blockSize;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY,
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
    path = new Path("newlyCreatedFile.txt");
    permission = new FsPermission((short)0666);
    bufferSize = 4096;
    replication = 3;
    blockSize = 512;
  }

  @After
  public void after() throws IOException {
    IOUtils.cleanup(LOG, grfs);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Tests for different CreateFlag combinations
   */
  @Test
  public void testCanCreateNewFileWithCreateFlagOnly()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    grfs.create(path, permission, flags, bufferSize, replication,
            blockSize, null);
    FileStatus[] files = grfs.listStatus(new Path("."));
    LOG.debug("list files under home dir");
    printFileStatus(files);
    assertEquals(1, files.length);
  }

  @Test
  public void testCreateExistedFileWithCreateFlagOnlyWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    FSDataOutputStream fsDataOutputStream = grfs.create(path, permission,
            flags, bufferSize, replication, blockSize, null);
    fsDataOutputStream.close();

    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (FileAlreadyExistsException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(1, files.length); // check if create file by mistake
    }
  }

  @Test  // opened means not closed yet
  public void testCreateOpenedFileWithCreateFlagOnlyWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    grfs.create(path, permission, flags, bufferSize, replication,
            blockSize, null);

    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (AlreadyBeingCreatedException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(1, files.length); // check if create file by mistake
    }
  }


  @Test
  public void testCreateFileWithEmptyFlagWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IllegalArgumentException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(0, files.length); // check if create file by mistake
    }
  }

  // Note, we do not support Append now so it throws
  // java.io.IOException: java.io.IOException: Append is not supported.
  // It will throw HadoopIllegalArgumentException if we support Append
  // in the future
  @Test
  public void testCreateFileWithAllFlagWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE, OVERWRITE, APPEND);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IOException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(0, files.length); // check if create file by mistake
    }
  }

  // Note, we do not support Append now so it throws
  // java.io.IOException: java.io.IOException: Append is not supported.
  // It will throw HadoopIllegalArgumentException if we support Append
  // in the future
  @Test
  public void testCreateFileWithAppendAndOverwriteFlagSetWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(OVERWRITE, APPEND);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IOException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(0, files.length); // check if create file by mistake
    }
  }

  // Note, we do not support Append now so it throws
  // java.io.IOException: java.io.IOException: Append is not supported.
  // It will throw FileNotFoundException if we support Append
  // in the future
  @Test
  public void testAppendNewFileWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(APPEND);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IOException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(0, files.length); // check if create file by mistake
    }
  }

  // Note, we do not support Append now so it throws
  // java.io.IOException: java.io.IOException: Append is not supported.
  // It should be fine in the future
  @Test
  public void testAppendExistedFileWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    FSDataOutputStream fsDataOutputStream = grfs.create(path, permission,
            flags, bufferSize, replication, blockSize, null);
    fsDataOutputStream.close();

    flags = EnumSet.of(APPEND);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IOException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(1, files.length); // check if create file by mistake
    }
  }

  // Note, we do not support Append now so it throws
  // java.io.IOException: java.io.IOException: Append is not supported.
  // It should be fine in the future
  @Test
  public void testAppendOpenedFileWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    grfs.create(path, permission, flags, bufferSize, replication,
            blockSize, null);

    flags = EnumSet.of(APPEND);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IOException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(1, files.length); // check if create file by mistake
    }
  }

  // Note, we do not support Append now so it throws
  // java.io.IOException: java.io.IOException: Append is not supported.
  // It should be fine in the future
  @Test
  public void testCreateNewFileWithAppendFlagWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE, APPEND);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IOException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(0, files.length); // check if create file by mistake
    }
  }

  // Note, we do not support Append now so it throws
  // java.io.IOException: java.io.IOException: Append is not supported.
  // It should be fine in the future
  @Test
  public void testCreateExistedFileWithAppendFlagWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    FSDataOutputStream fsDataOutputStream = grfs.create(path, permission,
            flags, bufferSize, replication, blockSize, null);
    fsDataOutputStream.close();

    flags = EnumSet.of(CREATE, APPEND);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IOException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(1, files.length); // check if create file by mistake
    }
  }

  // Note, we do not support Append now so it throws
  // java.io.IOException: java.io.IOException: Append is not supported.
  // It should be fine in the future
  @Test
  public void testCreateOpenedFileWithAppendFlagWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    grfs.create(path, permission, flags, bufferSize, replication,
            blockSize, null);

    flags = EnumSet.of(CREATE, APPEND);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (IOException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(1, files.length); // check if create file by mistake
    }
  }

  @Test
  public void testOverwriteNonExistedFileWillGetException()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(OVERWRITE);
    try {
      grfs.create(path, permission, flags, bufferSize, replication,
              blockSize, null);
      assertFalse(true);  // should never come here
    } catch (FileNotFoundException e)  {
      // That's what we need
    } finally {
      FileStatus[] files = grfs.listStatus(new Path("."));
      assertEquals(0, files.length); // check if create file by mistake
    }
  }

  @Test
  public void testCanOverwriteExistedFile()
          throws IOException {
    final int LEN1 = 2000;
    final int LEN2 = 1000;
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    FSDataOutputStream fsDataOutputStream = grfs.create(path, permission,
            flags, bufferSize, replication, blockSize, null);
    for(int j = 0; j < LEN1; j++) {
      fsDataOutputStream.write('c');
    }
    fsDataOutputStream.close();

    flags = EnumSet.of(OVERWRITE);
    fsDataOutputStream = grfs.create(path, permission, flags, bufferSize,
            replication, blockSize, null);
    for(int j = 0; j < LEN2; j++) {
      fsDataOutputStream.write('c');
    }
    fsDataOutputStream.close();

    FileStatus files[] = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(1, files.length);
    assertEquals(LEN2, files[0].getLen());
  }

  @Test
  public void testCanCreateNewFileWithCreateAndOverwriteFlag()
          throws IOException {
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE, OVERWRITE);
    grfs.create(path, permission, flags, bufferSize, replication,
            blockSize, null);

    FileStatus files[] = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(1, files.length);
  }

  @Test
  public void testCanCreateExistedFileWithCreateAndOverwriteFlag()
          throws IOException {
    final int LEN1 = 2000;
    final int LEN2 = 1000;
    EnumSet<CreateFlag> flags = EnumSet.of(CREATE);
    FSDataOutputStream fsDataOutputStream = grfs.create(path, permission,
            flags, bufferSize, replication, blockSize, null);
    for(int j = 0; j < LEN1; j++) {
      fsDataOutputStream.write('c');
    }
    fsDataOutputStream.close();

    flags = EnumSet.of(CREATE,OVERWRITE);
    fsDataOutputStream = grfs.create(path, permission, flags, bufferSize,
            replication, blockSize, null);
    for(int j = 0; j < LEN2; j++) {
      fsDataOutputStream.write('c');
    }
    fsDataOutputStream.close();

    FileStatus files[] = grfs.listStatus(new Path("."));
    printFileStatus(files);
    assertEquals(1, files.length);
    assertEquals(LEN2, files[0].getLen());
  }

  public static void main(String[] args) throws Exception {
    TestCreate test = new TestCreate();
    GiraffaConfiguration conf =
      new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaFileSystem.format(conf, true);
    GiraffaTestUtils.setGiraffaURI(conf);
    grfs = (GiraffaFileSystem) FileSystem.get(conf);
    test.testTheFileSystemShouldBeEmptyWhenInit();
  }
}