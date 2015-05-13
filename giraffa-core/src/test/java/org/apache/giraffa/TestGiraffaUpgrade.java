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

import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.giraffa.hbase.INodeManager;
import org.apache.giraffa.hbase.NamespaceProcessor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGiraffaUpgrade {
  private final static Logger LOG = LoggerFactory.getLogger(TestGiraffaUpgrade.class);

  private static final String TEST_IMAGE_FILE_OUT =
          GiraffaTestUtils.BASE_TEST_DIRECTORY+"/testFsImageOut";
  private static final HBaseTestingUtility UTIL =
      GiraffaTestUtils.getHBaseTestingUtility();
  private DFSTestUtil fsUtil;
  private GiraffaFileSystem grfa;
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
    grfa = (GiraffaFileSystem) FileSystem.get(conf);
    TableName tableName =
        TableName.valueOf(conf.get(GiraffaConfiguration.GRFA_TABLE_NAME_KEY,
            GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT));
    HRegion hRegion = UTIL.getHBaseCluster().getRegions(tableName).get(0);
    CoprocessorEnvironment env = hRegion.getCoprocessorHost()
        .findCoprocessorEnvironment(NamespaceProcessor.class.getName());
    nodeManager = new INodeManager(conf, env);
  }

  @After
  public void after() throws IOException {
    if(grfa != null) grfa.close();
    nodeManager.close();
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
    assertTrue(fsUtil.checkFiles(grfa, "generateFsImage"));

    FileStatus[] stats = grfa.listStatus(new Path("/"));
    for (FileStatus stat : stats) {
      LOG.debug(stat.getPath().getName());
    }
  }

  private String generateFsImage() throws IOException {
    MiniDFSCluster dfsCluster = UTIL.getDFSCluster();
    NameNode nn = dfsCluster.getNameNode();
    FileSystem dfs = dfsCluster.getFileSystem();
    fsUtil = new DFSTestUtil("generateFsImage", 100, 5, 4096, 0);
    fsUtil.createFiles(dfs, "generateFsImage");
    NameNodeAdapter.enterSafeMode(nn, false);
    NameNodeAdapter.saveNamespace(nn);
    NameNodeAdapter.leaveSafeMode(nn);
    Collection<URI> namespaceDirs = dfsCluster.getNameDirs(0);
    String namespaceDir = namespaceDirs.iterator().next().getRawPath();
    long txid = nn.getNamesystem().getFSImage().getMostRecentCheckpointTxId();
    return namespaceDir + "/current/" + NNStorage.getImageFileName(txid);
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
        List<UnlocatedBlock> blocks = new ArrayList<UnlocatedBlock>(1);
        List<DatanodeInfo[]> locations = new ArrayList<DatanodeInfo[]>(1);
        boolean isDirectory = false;
        long length = 0;
        if(numOfBlocks != -1) {
          length = parseBlocks(numOfBlocks, blocks, locations, br, path);
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
        INode node = new INode(length, isDirectory, replication, blockSize,
            modTime.getTime(), accessTime.getTime(), perm, userName,
            groupName, null, RowKeyFactory.newInstance(path), dsQuota, nsQuota,
            FileState.CLOSED, RenameState.FALSE(), blocks, locations, null);
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

  private long parseBlocks(int numOfBlocks, List<UnlocatedBlock> blocks,
      List<DatanodeInfo[]> locations, BufferedReader br, String path)
          throws IOException {
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
    String line = br.readLine().replace("      BLOCKS [NUM_BLOCKS = ", "")
        .replace("]", "").trim();
    return Integer.parseInt(line);
  }
}
