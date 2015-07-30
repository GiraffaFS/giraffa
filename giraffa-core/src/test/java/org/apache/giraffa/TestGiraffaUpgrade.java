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

import java.io.BufferedReader;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.giraffa.hbase.INodeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGiraffaUpgrade {
  static final Log LOG = LogFactory.getLog(TestGiraffaUpgrade.class);

  private static final String TEST_IMAGE_FILE_OUT =
      GiraffaTestUtils.BASE_TEST_DIRECTORY+"/testFsImageOut";
  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private DFSTestUtil fsUtil;
  private GiraffaFileSystem grfs;
  private Connection connection;
  private INodeManager nodeManager;

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
    nodeManager = GiraffaTestUtils.getNodeManager(conf, connection);
  }

  @After
  public void after() throws IOException {
    IOUtils.cleanup(LOG, grfs, nodeManager, connection);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testUpgrade() throws Exception {
    String imageFile = generateFsImage();

    // OIV args: -i fsimage -o fsimage.txt -p Indented
    // we use Indented processor because it outputs blockIDs
    String[] args = new String[6];
    args[0] = "-i";
    args[1] = imageFile;
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
    assertTrue(fsUtil.checkFiles(grfs, "generateFsImage"));

    FileStatus[] stats = grfs.listStatus(new Path("/"));
    for (FileStatus stat : stats) {
      LOG.debug(stat.getPath().getName());
    }
  }

  private String generateFsImage() throws IOException {
    MiniDFSCluster dfsCluster = UTIL.getDFSCluster();
    NameNode nn = dfsCluster.getNameNode();
    FSNamesystem ns = NameNodeAdapter.getNamesystem(nn);
    FileSystem dfs = dfsCluster.getFileSystem();

    // create test files
    fsUtil = new DFSTestUtil("generateFsImage", 100, 5, 4096, 0);
    fsUtil.createFiles(dfs, "generateFsImage");

    // get directory to save image
    Collection<URI> namespaceDirs = dfsCluster.getNameDirs(0);
    String namespaceDir = namespaceDirs.iterator().next().getRawPath();
    String targetDir = namespaceDir + "/current/";

    // save image in legacy format
    NameNodeAdapter.enterSafeMode(nn, false);
    ns.getFSImage().saveLegacyOIVImage(ns, targetDir, new Canceler());
    NameNodeAdapter.leaveSafeMode(nn);

    // return image file path
    long txid = ns.getFSImage().getLastAppliedOrWrittenTxId();
    return targetDir + NNStorage.getLegacyOIVImageFileName(txid);
  }

  private boolean parseIndentedFsImageOut(BufferedReader br)
      throws IOException, ParseException {
    while(br.ready()) {
      String line = br.readLine().trim();
      if(line.equals("INODE")) {
        String path = parseLine(br, "INODE_PATH");
        if(path.isEmpty()) continue;
        parseLine(br, "INODE_ID");
        short replication = Short.parseShort(parseLine(br, "REPLICATION"));
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd kk:mm");
        Date modTime = df.parse(parseLine(br, "MODIFICATION_TIME"));
        Date accessTime = df.parse(parseLine(br, "ACCESS_TIME"));
        long blockSize = Long.parseLong(parseLine(br, "BLOCK_SIZE"));
        int numOfBlocks = getNumberOfBlocks(br);
        List<UnlocatedBlock> blocks = new ArrayList<UnlocatedBlock>(1);
        List<DatanodeInfo[]> locations = new ArrayList<DatanodeInfo[]>(1);
        boolean isDirectory = numOfBlocks == -1;

        long length = 0;
        long nsQuota = -1;
        long dsQuota = -1;
        if(isDirectory) {
          nsQuota = Long.parseLong(parseLine(br, "NS_QUOTA"));
          dsQuota = Long.parseLong(parseLine(br, "DS_QUOTA"));
          parseLine(br, "IS_WITHSNAPSHOT_DIR");
        } else {
          length = parseBlocks(numOfBlocks, blocks, locations, br, path);
        }

        // Fetch permissions information
        String perms = br.readLine().trim();
        assertTrue("Permissions were not next; corrupt FsImageOut?",
            perms.equals("PERMISSIONS"));
        String userName = parseLine(br, "USER_NAME");
        String groupName = parseLine(br, "GROUP_NAME");
        FsPermission perm = FsPermission.valueOf("-" +
            parseLine(br, "PERMISSION_STRING"));

        // COMMIT IT!
        INode node;
        RowKey key = RowKeyFactory.newInstance(path);
        if (isDirectory) {
          node = new INodeDirectory(key, modTime.getTime(),
              accessTime.getTime(), userName, groupName, perm, null, null,
              dsQuota, nsQuota);
        } else {
          node = new INodeFile(key, modTime.getTime(), accessTime.getTime(),
              userName, groupName, perm, null, null, length, replication,
              blockSize, FileState.CLOSED, null, blocks, locations);
        }
        try {
          nodeManager.updateINode(node);
          LOG.debug("COMMITTED: " + path + ", with BLOCKS:" + blocks);
        } catch(IOException e) {
          LOG.error("Failed to commit INODE: "+path, e);
          fail();
        }
      }
    }
    return true;
  }

  private static String parseLine(BufferedReader br, String key)
      throws IOException {
    return br.readLine().replace(key + " = ", "").trim();
  }

  private long parseBlocks(int numOfBlocks, List<UnlocatedBlock> blocks,
      List<DatanodeInfo[]> locations, BufferedReader br, String path)
          throws IOException {
    long totalLength = 0;
    for(int i = 0; i < numOfBlocks; i++) {
      String blockLine = br.readLine().trim();
      assertTrue(blockLine.equals("BLOCK"));
      long blockID = Long.parseLong(parseLine(br, "BLOCK_ID"));
      long blockLength = Long.parseLong(parseLine(br, "NUM_BYTES"));
      long genStamp = Long.parseLong(parseLine(br, "GENERATION_STAMP"));
      totalLength += blockLength;
      assertTrue("Wrong block id", blockID != 0);
      assertTrue("Wrong block genStamp", genStamp > 0);
    }
    // PJJ: Block locations need to be fetched differently
    // This is just a work around for now to make it work.
    MiniDFSCluster dfsCluster = UTIL.getDFSCluster();
    LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(
        dfsCluster.getNameNode(), path, 0, totalLength);
    for(LocatedBlock lb : lbs.getLocatedBlocks()) {
      blocks.add(new UnlocatedBlock(lb));
      locations.add(lb.getLocations());
    }
    
    return totalLength;
  }

  private int getNumberOfBlocks(BufferedReader br) throws IOException {
    String line = parseLine(br, "BLOCKS [NUM_BLOCKS").replace("]", "");
    return Integer.parseInt(line);
  }
}
