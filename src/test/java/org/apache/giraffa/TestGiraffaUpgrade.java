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

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGiraffaUpgrade {
  private static MiniHBaseCluster cluster;
  private static final String TEST_IMAGE_FILE_OUT =
          GiraffaTestUtils.BASE_TEST_DIRECTORY+"/testFsImageOut";
  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private DFSTestUtil fsUtil;
  private GiraffaFileSystem grfa;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // delete fsImageOut if it exists
    File testImage = new File(TEST_IMAGE_FILE_OUT);
    if(testImage.exists()) {
      assertTrue(testImage.delete());
    }
    // setup MiniCluster properties
    System.setProperty(
        HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, GiraffaTestUtils.BASE_TEST_DIRECTORY);
    cluster = UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws IOException {
    GiraffaConfiguration conf =
        new GiraffaConfiguration(UTIL.getConfiguration());
    conf.set(GiraffaConfiguration.GRFA_JAR_FILE_KEY, GiraffaTestUtils.TEST_GRFA_JAR_FILE);
    GiraffaTestUtils.setGiraffaURI(conf);
    GiraffaFileSystem.format(conf, false);
    grfa = (GiraffaFileSystem) FileSystem.get(conf);
  }

  @After
  public void after() throws IOException {
    if(grfa != null) grfa.close();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (cluster != null) cluster.shutdown();
  }

  @Test
  public void testUpgrade() throws Exception {
    String namespaceDir = generateFsImage();

    // OIV args: -i fsimage -o fsimage.txt -p Indented
    // we use Indented processor because it outputs blockIDs
    String[] args = new String[6];
    args[0] = "-i";
    args[1] = namespaceDir + "/current/fsimage";
    args[2] = "-o";
    args[3] = TEST_IMAGE_FILE_OUT;
    args[4] = "-p";
    args[5] = "Indented";
    OfflineImageViewer.main(args);

    // we now have the indented fsImage, write it into GRFA!
    BufferedReader br = new BufferedReader(new FileReader(TEST_IMAGE_FILE_OUT));
    assertTrue(parseIndentedFsImageOut(br));
    br.close();

    // check that the files appear in GRFA!
    assertTrue(fsUtil.checkFiles(grfa, "generateFsImage"));

    FileStatus[] stats = grfa.listStatus(new Path("/"));
    for (FileStatus stat : stats) {
      System.out.println(stat.getPath().getName());
    }
  }

  private String generateFsImage() throws IOException {
    MiniDFSCluster dfsCluster = UTIL.getDFSCluster();
    NameNode nn = dfsCluster.getNameNode();
    FileSystem dfs = dfsCluster.getFileSystem();
    fsUtil = new DFSTestUtil("generateFsImage", 100, 5, 4096);
    fsUtil.createFiles(dfs, "generateFsImage");
    nn.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_ENTER);
    nn.saveNamespace();
    nn.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
    Collection<URI> namespaceDirs = dfsCluster.getNameDirs();
    return namespaceDirs.iterator().next().getRawPath();
  }

  private boolean parseIndentedFsImageOut(BufferedReader br)
      throws IOException, ParseException {
    while(br.ready()) {
      String line = br.readLine();
      if(line.equals("    INODE")) {
        String path = br.readLine().replace("      INODE_PATH = ", "").trim();
        if(path.isEmpty()) continue;
        short replication =
            Short.parseShort(br.readLine().replace("      REPLICATION = ", "").trim());
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd kk:mm");
        Date modTime =
            df.parse(br.readLine().replace("      MODIFICATION_TIME = ", "").trim());
        Date accessTime =
            df.parse(br.readLine().replace("      ACCESS_TIME = ", "").trim());
        long blockSize =
            Long.parseLong(br.readLine().replace("      BLOCK_SIZE = ", "").trim());
        int numOfBlocks = getNumberOfBlocks(br);
        List<LocatedBlock> blocks = new ArrayList<LocatedBlock>(1);
        boolean isDirectory = false;
        long length = 0;
        if(numOfBlocks != -1) {
          length = parseBlocks(numOfBlocks, blocks, br, path);
        } else {
          isDirectory = true;
        }

        long nsQuota = -1;
        long dsQuota = -1;

        // When numOfBlocks == 0 there is no Quota information
        if(numOfBlocks != 0) {
          nsQuota =
              Long.parseLong(br.readLine().replace("      NS_QUOTA = ", "").trim());
          dsQuota =
              Long.parseLong(br.readLine().replace("      DS_QUOTA = ", "").trim());
        }

        // Fetch permissions information
        String perms = br.readLine();
        assertTrue("Permissions were not next; corrupt FsImageOut?",
            perms.equals("      PERMISSIONS"));
        String userName = br.readLine().replace("        USER_NAME = ", "").trim();
        String groupName = br.readLine().replace("        GROUP_NAME = ", "").trim();
        FsPermission perm = FsPermission.valueOf("-"+br.readLine()
             .replace("        PERMISSION_STRING = ","").trim());

        // COMMIT IT!
        commitToHBase(path, replication, modTime, accessTime, blockSize, blocks,
            isDirectory, nsQuota, dsQuota, userName, groupName, perm, length);
      }
    }
    return true;
  }

  private void commitToHBase(String path, short replication, Date modTime,
                             Date accessTime, long blockSize,
                             List<LocatedBlock> blocks, boolean directory,
                             long nsQuota, long dsQuota, String userName,
                             String groupName, FsPermission perm, long length) {
    try {
      HTable table = new HTable(cluster.getConfiguration(),
          GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT.getBytes());

      long ts = now();
      RowKey key = new FullPathRowKey(path);
      Put put = new Put(key.getKey(), ts);
      put.add(FileField.getFileAttributes(), FileField.getFileName(), ts,
          new Path(path).getName().getBytes())
          .add(FileField.getFileAttributes(), FileField.getUserName(), ts,
              userName.getBytes())
          .add(FileField.getFileAttributes(), FileField.getGroupName(), ts,
              groupName.getBytes())
          .add(FileField.getFileAttributes(), FileField.getLength(), ts,
              Bytes.toBytes(length))
          .add(FileField.getFileAttributes(), FileField.getPermissions(), ts,
              Bytes.toBytes(perm.toShort()))
          .add(FileField.getFileAttributes(), FileField.getMTime(), ts,
              Bytes.toBytes(modTime.getTime()))
          .add(FileField.getFileAttributes(), FileField.getATime(), ts,
              Bytes.toBytes(accessTime.getTime()))
          .add(FileField.getFileAttributes(), FileField.getDsQuota(), ts,
              Bytes.toBytes(dsQuota))
          .add(FileField.getFileAttributes(), FileField.getNsQuota(), ts,
              Bytes.toBytes(nsQuota))
          .add(FileField.getFileAttributes(), FileField.getReplication(), ts,
              Bytes.toBytes(replication))
          .add(FileField.getFileAttributes(), FileField.getBlockSize(), ts,
              Bytes.toBytes(blockSize));

      if(directory)
        put.add(FileField.getFileAttributes(), FileField.getDirectory(), ts,
            Bytes.toBytes(directory));
      else
        put.add(FileField.getFileAttributes(), FileField.getBlock(), ts,
            getBlocksBytes(blocks))
            .add(FileField.getFileAttributes(), FileField.getState(), ts,
                Bytes.toBytes(GiraffaConstants.FileState.CLOSED.toString()));

      synchronized(table) {
        table.put(put);
        table.close();
      }
      System.out.println("COMMITED: "+path+", with BLOCKS:"+blocks);
    } catch (IOException e) {
      System.err.println("Failed to commit INODE: "+path);
      e.printStackTrace();
      fail();
    }
  }

  private byte[] getBlocksBytes(List<LocatedBlock> blocks) throws IOException {
    if(blocks == null) return null;
    byte[] retVal = null;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    try {
      for(LocatedBlock loc : blocks) {
        loc.write(out);
      }
      retVal = baos.toByteArray();
    } finally {
      out.close();
    }
    return retVal;
  }

  private long parseBlocks(int numOfBlocks, List<LocatedBlock> blocks,
                           BufferedReader br, String path) throws IOException {
    long totalLength = 0;
    for(int i = 0; i < numOfBlocks; i++) {
      String blockLine = br.readLine();
      assertTrue(blockLine.equals("        BLOCK"));
      long blockID =
          Long.parseLong(br.readLine().replace("          BLOCK_ID = ", "").trim());
      long blockLength =
          Long.parseLong(br.readLine().replace("          NUM_BYTES = ", "").trim());
      long genStamp =
          Long.parseLong(br.readLine().replace("          GENERATION_STAMP = ", "").trim());
      totalLength += blockLength;
    }
    // PJJ: We need to replace LocatedBlocks in Giraffa with Blocks & Locations.
    // This is just a work around for now to make it work.
    MiniDFSCluster dfsCluster = UTIL.getDFSCluster();
    LocatedBlocks lbs =
        dfsCluster.getNameNode().getBlockLocations(path, 0, totalLength);
    blocks.addAll(lbs.getLocatedBlocks());
    return totalLength;
  }

  private int getNumberOfBlocks(BufferedReader br) throws IOException {
    String line = br.readLine().replace("      BLOCKS [NUM_BLOCKS = ", "")
        .replace("]", "").trim();
    return Integer.parseInt(line);
  }
}
