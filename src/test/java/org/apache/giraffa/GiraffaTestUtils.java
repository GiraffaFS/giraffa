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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class GiraffaTestUtils {
  static final HBaseTestingUtility HB_UTIL = new HBaseTestingUtility();
  public static final String BASE_TEST_DIRECTORY = "target/build/test-data";
  public static final String TEST_GRFA_JAR_FILE = System.getProperty("grfa.test.jar.file");

  public static URI getGiraffaURI() throws IOException {
    try {
      return new URI(GiraffaConfiguration.GRFA_URI_SCHEME + ":///");
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public static void setGiraffaURI(Configuration conf) throws IOException {
    FileSystem.setDefaultUri(conf, getGiraffaURI());
  }

  public static HBaseTestingUtility getHBaseTestingUtility() {
    return new HBaseTestingUtility();
  }

  static void listStatusRecursive(
      ArrayList<FileStatus> results, FileSystem fs, Path f
      ) throws FileNotFoundException, IOException {
    FileStatus listing[] = fs.listStatus(f);

    for(FileStatus file : listing) {
      results.add(file);
      if(file.isDirectory())
        listStatusRecursive(results, fs, file.getPath());
    }
  }

  public static FileStatus[] listStatusRecursive(FileSystem fs, Path f)
  throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus> ();
    listStatusRecursive(results, fs, f);
    return results.toArray(new FileStatus[results.size()]);
  }

  public static void printFileStatus(FileStatus fileStat) throws IOException {
    printFileStatus(fileStat, -1);
  }

  public static void printFileStatus(FileStatus fileStat, int i)
      throws IOException {
    System.out.println(
        "=============== FILE STATUS " + (i>=0?i:"") + " ===============");
    System.out.println("OWNER: " + fileStat.getOwner());
    System.out.println("GROUP: " + fileStat.getGroup());
    System.out.println("PATH: " + fileStat.getPath());
    System.out.println("PERMS: " + fileStat.getPermission().toString());
    System.out.println("LEN: " + fileStat.getLen());
    System.out.println("ATIME: " + fileStat.getAccessTime());
    System.out.println("MTIME: " + fileStat.getModificationTime());
    System.out.println("BLKSIZE: " + fileStat.getBlockSize());
    System.out.println("REPL: " + fileStat.getReplication());
    if(fileStat.isSymlink())
      System.out.println("SYMLINK: " + fileStat.getSymlink());
  }

  public static void printFileStatus(FileStatus[] fileStat) throws IOException {
    for (int i = 0; i < fileStat.length; i++) {
      printFileStatus(fileStat[i], i);
    }
  }
}
