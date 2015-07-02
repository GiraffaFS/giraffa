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
package org.apache.giraffa.hbase;

import java.util.Collection;
import static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName;
import static org.apache.giraffa.GiraffaConstants.BlockAction;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.util.Time.now;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FSPermissionChecker;
import org.apache.giraffa.FileLease;
import org.apache.giraffa.GiraffaConfiguration;
import org.apache.giraffa.LeaseManager;
import org.apache.giraffa.INode;
import org.apache.giraffa.RenameState;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.RowKeyFactory;
import org.apache.giraffa.UnlocatedBlock;
import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.giraffa.hbase.INodeManager.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.ipc.HBaseRpcUtil;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;

import com.google.protobuf.Service;

/**
  */
public class NamespaceProcessor implements ClientProtocol,
    Coprocessor, CoprocessorService {
  // RPC service fields
  ClientNamenodeProtocolServerSideCallbackTranslatorPB translator =
      new ClientNamenodeProtocolServerSideCallbackTranslatorPB(this);
  Service service = ClientNamenodeProtocol.newReflectiveService(translator);

  private INodeManager nodeManager;
  private XAttrOp xAttrOp;

  private LeaseManager leaseManager;
  private Daemon monitor;
  private FsServerDefaults serverDefaults;
  private boolean running;

  private int lsLimit;

  private static final Log LOG =
      LogFactory.getLog(NamespaceProcessor.class.getName());
   
  private String fsOwnerShortUserName;
  private String supergroup;
  private boolean isPermissionEnabled;
  private boolean xattrsEnabled;

  public NamespaceProcessor() {}
  
  @Override // CoprocessorService
  public Service getService() {
    return service;
  }

  @Override // Coprocessor
  public void start(CoprocessorEnvironment env) throws IOException {    
    if (!(env instanceof RegionCoprocessorEnvironment)) {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
    RegionCoprocessorEnvironment e = (RegionCoprocessorEnvironment) env;
    LOG.info("Start NamespaceProcessor...");
    Configuration conf = e.getConfiguration();

    UserGroupInformation fsOwner = UserGroupInformation.getCurrentUser();
    fsOwnerShortUserName = fsOwner.getShortUserName();
    supergroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
        DFS_PERMISSIONS_ENABLED_DEFAULT);
    LOG.info("fsOwner             = " + fsOwner);
    LOG.info("supergroup          = " + supergroup);
    LOG.info("isPermissionEnabled = " + isPermissionEnabled);
    xattrsEnabled = conf.getBoolean(DFS_NAMENODE_XATTRS_ENABLED_KEY,
        DFS_NAMENODE_XATTRS_ENABLED_DEFAULT);
    LOG.info("xattrsEnabled = " + xattrsEnabled);

    RowKeyFactory.registerRowKey(conf);
    int configuredLimit = conf.getInt(
        GiraffaConfiguration.GRFA_LIST_LIMIT_KEY,
        GiraffaConfiguration.GRFA_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit > 0 ?
        configuredLimit : GiraffaConfiguration.GRFA_LIST_LIMIT_DEFAULT;
    LOG.info("Caching is set to: " + RowKeyFactory.isCaching());
    LOG.info("RowKey is set to: " +
        RowKeyFactory.getRowKeyClass().getCanonicalName());
    
    // Get the checksum type from config
    String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY,
                                      DFS_CHECKSUM_TYPE_DEFAULT);
    DataChecksum.Type checksumType;
    try {
       checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
    } catch (IllegalArgumentException iae) {
       throw new IOException("Invalid checksum type in "
          + DFS_CHECKSUM_TYPE_KEY + ": " + checksumTypeStr);
    }

    TableName tableName = TableName.valueOf(getGiraffaTableName(conf));

    this.serverDefaults = new FsServerDefaults(
        conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
        conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
        conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
            DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
        (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
        conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
        conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY,
            DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
        conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
        checksumType);

    this.leaseManager =
        LeaseManager.originateSharedLeaseManager(e.getRegionServerServices()
            .getRpcServer().getListenerAddress().toString());
    this.nodeManager = new INodeManager(e.getTable(tableName));
    this.xAttrOp = new XAttrOp(nodeManager);
    this.monitor = leaseManager.getMonitor(this);
    leaseManager.startMonitor();
    this.running = true;
  }

  @Override // Coprocessor
  public void stop(CoprocessorEnvironment env) {
    LOG.info("Stopping NamespaceProcessor...");
    running = false;
    nodeManager.close();
    leaseManager.stopMonitor();
  }

  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
                           String holder)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    throw new IOException("abandonBlock is not supported");
  }

  @Override // ClientProtocol
  public LocatedBlock addBlock(String src, String clientName,
                               ExtendedBlock previous,
                               DatanodeInfo[] excludeNodes, long fileId,
                               String[] favoredNodes)
      throws AccessControlException, FileNotFoundException,
             NotReplicatedYetException, SafeModeException,
             UnresolvedLinkException, IOException {
    INode iNode = nodeManager.getINode(src);

    if(iNode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    // Calls addBlock on HDFS by putting another empty Block in HBase
    if(previous != null) {
      // we need to update in HBase the previous block
      iNode.setLastBlock(previous);
    }
    
    // add a Block and modify times
    // (if there was a previous block this call with add it in as well)
    long time = now();
    iNode.setTimes(time, time);
    nodeManager.updateINode(iNode, BlockAction.ALLOCATE);

    // grab blocks back from HBase and return the latest one added
    nodeManager.getBlocksAndLocations(iNode);
    List<UnlocatedBlock> al_blks = iNode.getBlocks();
    List<DatanodeInfo[]> al_locs = iNode.getLocations();

    if(al_blks.size() != al_locs.size()) {
      throw new IOException("Number of block infos (" + al_blks.size() +
          ") and number of location infos (" + al_locs.size() +
          ") do not match");
    }

    int last = al_blks.size() - 1;
    if(last < 0)
      throw new IOException("Number of block infos is 0");
    else
      return al_blks.get(last).toLocatedBlock(al_locs.get(last));
  }

  @Override // ClientProtocol
  public LocatedBlock append(String src, String clientName)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    throw new IOException("append is not supported");
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    throw new IOException("cancelDelegationToken is not supported");
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName,
                          ExtendedBlock last, long fileId)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    INode iNode = nodeManager.getINode(src);

    if(iNode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    checkLease(src, iNode, clientName);

    // set the state and replace the block, then put the iNode
    iNode.setState(FileState.CLOSED);
    leaseManager.removeLease(iNode.getLease());
    iNode.setLease(null);
    iNode.setLastBlock(last);
    long time = now();
    iNode.setTimes(time, time);
    if(last != null)
      nodeManager.updateINode(iNode, BlockAction.CLOSE);
    else
      nodeManager.updateINode(iNode);
    LOG.info("Completed file: " + src + " | Block: " + last);
    return true;
  }

  @Override // ClientProtocol
  public void concat(String trg, String[] srcs) throws IOException,
      UnresolvedLinkException {
    throw new IOException("concat is not supported");
  }

  @Override // ClientProtocol
  public HdfsFileStatus create(String src, FsPermission masked,
                               String clientName,
                               EnumSetWritable<CreateFlag> createFlag,
                               boolean createParent, short replication,
                               long blockSize)
      throws AccessControlException, AlreadyBeingCreatedException,
             DSQuotaExceededException, FileAlreadyExistsException,
             FileNotFoundException, NSQuotaExceededException,
             ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
             SnapshotAccessControlException, IOException {
    assertNotRoot(src);
    EnumSet<CreateFlag> flag = createFlag.get();
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean append = flag.contains(CreateFlag.APPEND);
    boolean create = flag.contains(CreateFlag.CREATE);

    assert (overwrite || append || create);
    if(append) {
      throw new IOException("Append is not supported.");
    }

    INode iFile = nodeManager.getINode(src);
    INode iParent = nodeManager.getParentINode(src);

    FSPermissionChecker pc = getFsPermissionChecker();
    if (isPermissionEnabled) {
      pc.check(iParent, FsAction.EXECUTE);
      if (overwrite && iFile != null) {
        pc.check(iFile, FsAction.WRITE);
      } else if (create && iFile == null) {
        pc.check(iParent, FsAction.WRITE);
      }
    }

    if(iFile != null) {
      if(iFile.isDir()) {
        throw new FileAlreadyExistsException(
            "File already exists as directory: " + src);
      } else if(overwrite) {
        if (!delete(iFile, false, true, null)) {
          throw new IOException("Cannot override existing file: " + src);
        }
        iFile = null; // now the file is deleted
      } else if(iFile.getFileState().equals(FileState.UNDER_CONSTRUCTION)) {
        // Opening an existing file for write - may need to recover lease.
        reassignLease(iFile, src, clientName, false);
      } else {
        throw new FileAlreadyExistsException();
      }
    } else if(overwrite && !create) {
      throw new FileNotFoundException("File not found: " + src);
    }

    if(!createParent && iParent == null) {
      throw new FileNotFoundException("Parent does not exist: " + src);
    }

    if (iParent == null) {
      // create parent directories, inherit permissions from ancestor
      Path parent = new Path(src).getParent();
      iParent = mkdirsRecursive(parent, null, true, pc);
    } else if(!iParent.isDir()) {
      throw new ParentNotDirectoryException("Parent path is not a directory: "
          + src);
    }

    // if file did not exist, create its INode now
    if(iFile == null) {
      RowKey key = RowKeyFactory.newInstance(src);
      long time = now();
      FileLease fileLease =
          leaseManager.addLease(new FileLease(clientName, src, time));
      iFile = new INode(0, false, replication, blockSize, time, time, masked,
          pc.getUser(), iParent.getGroup(), null, key, 0, 0,
          FileState.UNDER_CONSTRUCTION, null, null, null, fileLease);
    }

    // add file to HBase (update if already exists)
    nodeManager.updateINode(iFile);
    LOG.info("Created file: " + src);
    return iFile.getLocatedFileStatus();
  }

  @Override // ClientProtocol
  public void createSymlink(
      String target, String link, FsPermission dirPerm, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    throw new IOException("symlinks are not supported");
  }

  @Deprecated // ClientProtocol
  public boolean delete(String src) throws IOException {
    return delete(src, false);
  }

  /**
   * Delete file(s). If recursive, will start from the lowest subtree, and
   * working up the directory tree, breadth first. This is NOT atomic.
   * If any failure occurs along the way, the deletion process will stop.
   */
  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    assertNotRoot(src);
    INode node = nodeManager.getINode(src);
    INode parent = nodeManager.getParentINode(src);

    FSPermissionChecker pc = getFsPermissionChecker();
    if (isPermissionEnabled) {
      pc.check(parent, FsAction.WRITE_EXECUTE);
      pc.checkStickyBit(parent, node);
    }

    if(node == null)
      return false;
    if(parent == null)
      throw new FileNotFoundException("Parent does not exist.");
    if(!parent.isDir())
      throw new ParentNotDirectoryException("Parent is not a directory.");

    return delete(node, recursive, true, pc);
  }

  private boolean delete(INode node, boolean recursive, boolean deleteBlocks,
                         FSPermissionChecker pc)
      throws IOException {
    boolean result = node.isDir() ?
        deleteDirectory(node, recursive, deleteBlocks, pc) :
        deleteFile(node, deleteBlocks);

    // delete time penalty (workaround for HBASE-2256)
    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) {
      // do nothing
    }

    return result;
  }

  private boolean deleteFile(INode node, boolean deleteBlocks)
      throws IOException {
    if(deleteBlocks) {
      node.setState(FileState.DELETED);
      nodeManager.updateINode(node, BlockAction.DELETE);
    }

    // delete the child key atomically first
    nodeManager.delete(node);
    return true;
  }

  /** 
   * The psuedo-recursive function which first deletes all children within 
   * a directory, then the directory itself.
   * If any of the children cannot be deleted the directory itself will not 
   * be deleted as well.
   *
   * @param node the parent INode (a directory)
   * @param recursive whether to delete entire subtree
   * @return true if the directory was actually deleted
   * @throws AccessControlException
   * @throws FileNotFoundException
   * @throws UnresolvedLinkException
   * @throws IOException
   */
  private boolean deleteDirectory(INode node, boolean recursive,
                                  final boolean deleteBlocks,
                                  FSPermissionChecker pc)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    if(recursive) {
      List<INode> directories = nodeManager.getDirectories(node);

      if (isPermissionEnabled && pc != null) {
        for (INode dir : directories) {
          Boolean empty = dir.isEmpty();
          assert empty != null;
          if (!empty) {
            pc.check(dir, FsAction.ALL);
          }
        }
      }

      // start ascending the tree (breadth first, then depth)
      // we do this by iterating through directories in reverse
      ListIterator<INode> it = directories.listIterator(directories.size());
      while (it.hasPrevious()) {
        final List<INode> dirsToDelete = new ArrayList<INode>();
        nodeManager.map(it.previous(), new Function() {
          @Override
          public void apply(INode input) throws IOException {
            if (!input.isDir())
              deleteFile(input, deleteBlocks);
            else
              dirsToDelete.add(input);
          }
        });

        // perform delete (if non-empty)
        if(!dirsToDelete.isEmpty()) {
          nodeManager.delete(dirsToDelete);
        }
      }
    }
    else if(!nodeManager.isEmptyDirectory(node)) {
      throw new PathIsNotEmptyDirectoryException(node.getRowKey().getPath()
          + " is non empty");
    }

    // delete source directory
    nodeManager.delete(node);
    return true;
  }

  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    throw new IOException("upgrade is not supported");
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    throw new IOException("rollingUpgrade is not supported");
  }

  @Override // ClientProtocol
  public void fsync(String src, long inodeId, String client,
                    long lastBlockLength)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    throw new IOException("fsync is not supported.");
  }

  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    INode iNode = nodeManager.getINode(src);

    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
      pc.check(iNode, FsAction.READ);
    }

    if(iNode == null || iNode.isDir()) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    List<LocatedBlock> al = UnlocatedBlock.toLocatedBlocks(iNode.getBlocks(),
        iNode.getLocations());
    boolean underConstruction = (iNode.getFileState().equals(FileState.CLOSED));

    LocatedBlock lastBlock = al.size() == 0 ? null : al.get(al.size()-1);
    LocatedBlocks lbs = new LocatedBlocks(computeFileLength(al),
        underConstruction, al, lastBlock, underConstruction);
    return lbs;
  }

  private static long computeFileLength(List<LocatedBlock> al) {
    // does not matter if underConstruction or not so far.
    long n = 0;
    for(LocatedBlock bl : al) {
        n += bl.getBlockSize();
    }
    LOG.info("Block filesize sum is: " + n);
    return n;
  }

  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(path), FsAction.EXECUTE);
    }

    INode node = nodeManager.getINode(path);
    if(node == null) {
      throw new FileNotFoundException("Path does not exist: " + path);
    }
    if(node.isDir()) {
      return new ContentSummary(0L, 0L, 1L, node.getNsQuota(), 
          0L, node.getDsQuota());
    }
    throw new IOException("Path is not a directory: " + path);
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    throw new IOException("getDatanodeReport is not supported");
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    throw new IOException("getDelegationToken is not supported");
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
    }

    INode node = nodeManager.getINode(src);
    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    return node.getFileStatus();
  }

  @Override
  public boolean isFileClosed(String src)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
    }

    INode node = nodeManager.getINode(src);
    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    return node.getFileState() == FileState.CLOSED;
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    throw new IOException("symlinks are not supported");
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws AccessControlException,
      FileNotFoundException, IOException {
    throw new IOException("symlinks are not supported");
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(
      String src, byte[] startAfter, boolean needLocation)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    INode node = nodeManager.getINode(src);

    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
      if (node != null && node.isDir()) {
        pc.check(node, FsAction.READ_EXECUTE);
      }
    }

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    if(!node.isDir()) {
      return new DirectoryListing(new HdfsFileStatus[] { (needLocation) ?
          node.getLocatedFileStatus() : node.getFileStatus() }, 0);
    }

    List<INode> list = nodeManager.getListing(node, startAfter, lsLimit);

    HdfsFileStatus[] retVal = new HdfsFileStatus[list.size()];
    int i = 0;
    for(INode child : list)
      retVal[i++] = (needLocation) ? child.getLocatedFileStatus() :
          child.getFileStatus();
    // We can say there is no more entries if the lsLimit is exhausted,
    // otherwise we know only that there could be one more entry
    return new DirectoryListing(retVal, list.size() < lsLimit ? 0 : 1);
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    throw new IOException("snapshots are not supported");
  }

  @Override // ClientProtocol
  public long getPreferredBlockSize(String src) throws IOException,
      UnresolvedLinkException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
    }

    INode inode = nodeManager.getINode(src);
    if(inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    return inode.getBlockSize();
  }

  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    return this.serverDefaults;
  }

  @Override // ClientProtocol
  public long[] getStats() throws IOException {
    throw new IOException("getStats is not supported");
  }

  @Override // ClientProtocol
  public void metaSave(String filename) throws IOException {
    throw new IOException("metaSave is not supported");
  }

  @Override // ClientProtocol
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    assertNotRoot(src);
    INode iParent = nodeManager.getParentINode(src);

    FSPermissionChecker pc = getFsPermissionChecker();
    if (isPermissionEnabled) {
      pc.check(iParent, FsAction.WRITE_EXECUTE);
    }

    RowKey key = RowKeyFactory.newInstance(src);
    INode inode = nodeManager.getINode(key);

    if(inode != null) {  // already exists
      return true;
    }

    // create parent directories if requested
    if(!createParent && iParent == null) {
      String parent = new Path(src).getParent().toString();
      throw new FileNotFoundException("Parent does not exist: "+parent);
    }
    if(iParent != null && !iParent.isDir()) {
      String parent = new Path(src).getParent().toString();
      throw new ParentNotDirectoryException("Parent is not directory: "+parent);
    }
    if(createParent && iParent == null) {
      // make the parent directories, use given permissions
      Path parent = new Path(src).getParent();
      iParent = mkdirsRecursive(parent, masked, false, pc);
    } 

    long time = now();
    inode = new INode(0, true, (short) 0, 0, time, time, masked, pc.getUser(),
        iParent.getGroup(), null, key, 0, 0, null, null, null, null, null);

    // add directory to HBase
    nodeManager.updateINode(inode);
    return true;
  }

  /**
   * Recursively creates a directory and any nonexisting parents. This method
   * assumes that a file or directory does not already exist at the given path.
   * @param src path of directory to create. A file or directory must not
   *            already exist at this location.
   * @param masked permission to set for <code>src</code> and created parents.
   *               Write and execute permission for user are automatically set
   *               as well. Can be <code>null</code> if
   *               <code>inheritPermissions</code> is <code>true</code>.
   * @param inheritPermissions if <code>true</code>, ignores <code>masked</code>
   *                           and instead sets permission for each created
   *                           directory equal to its parent's permission, plus
   *                           write and execute permission for user.
   * @param pc for checking write and execute permission of parents of created
   *           directories. Can be <code>null</code> if permissions are
   *           disabled.
   * @return INode of newly created <code>src</code>
   */
  private INode mkdirsRecursive(Path src, FsPermission masked,
                                boolean inheritPermissions,
                                FSPermissionChecker pc)
      throws IOException {
    Path parent = src.getParent();
    INode iParent = parent == null ? null :
        nodeManager.getINode(parent.toString());

    if (isPermissionEnabled) {
      pc.check(iParent, FsAction.WRITE_EXECUTE);
    }

    if (iParent != null && !iParent.isDir()) {
      throw new ParentNotDirectoryException(src.toString());
    }
    if (iParent == null && parent != null) {
      iParent = mkdirsRecursive(parent, masked, inheritPermissions, pc);
    }

    RowKey key = RowKeyFactory.newInstance(src.toString());
    long time = now();
    String user, group;
    if (parent == null) {
      // root directory settings
      user = fsOwnerShortUserName;
      group = supergroup;
      masked = FsPermission.createImmutable((short)0755);
    } else {
      user = pc.getUser();
      group = iParent.getGroup();
      masked = setUWX(inheritPermissions ? iParent.getPermission() : masked);
    }

    INode inode = new INode(0, true, (short) 0, 0, time, time, masked, user,
        group, null, key, 0, 0, null, null, null, null, null);
    nodeManager.updateINode(inode);
    return inode;
  }

  /**
   * Return a new FsPermission with write and execute for user enabled.
   */
  private FsPermission setUWX(FsPermission permission) {
    return new FsPermission(
        permission.getUserAction().or(FsAction.WRITE_EXECUTE),
        permission.getGroupAction(),
        permission.getOtherAction());
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName) throws IOException {
    Collection<FileLease> leases = leaseManager.getLeases(clientName);
    if(leases == null)
      return false;
    for(FileLease lease : leases) {
      if(lease.getPath().equals(src)) {
        return internalReleaseLease(lease, src);
      }
    }
    return false;
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    throw new IOException("refreshNodes is not supported");
  }

  @Override // ClientProtocol
  public boolean rename(String src, String dst) throws UnresolvedLinkException,
      IOException {
    try {
      rename2(src, dst);
    } catch (AccessControlException e) {
      throw e;
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return false;
    }
    return true;
  }

  @Override // ClientProtocol
  public void rename2(String src, String dst, Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    LOG.info("Renaming " + src + " to " + dst);
    INode rootSrcNode = nodeManager.getINode(src);
    INode rootDstNode = nodeManager.getINode(dst);
    boolean overwrite = false;

    for(Rename option : options) {
      if(option.equals(Rename.OVERWRITE))
        overwrite = true;
    }

    checkCanRename(src, rootSrcNode, dst, rootDstNode, overwrite);

    boolean directoryRename = rootSrcNode != null && rootSrcNode.isDir() ||
        rootDstNode != null && rootDstNode.isDir();
    if(directoryRename)
      LOG.debug("Detected directory rename");

    if(rootDstNode != null && !rootDstNode.getRenameState().getFlag() &&
        overwrite) {
      LOG.debug("Overwriting "+dst);
      if(!delete(dst, false)) // will fail if dst is non-empty directory
        throw new IOException("rename cannot overwrite non empty destination "+
            "directory " + dst);
      rootDstNode = null;
    }

    // Stage 1: copy into new row with RenameState flag
    if(rootDstNode == null) {
      if(directoryRename) { // first do Stage 1 for all children
        final URI base = new Path(src).toUri();
        final URI newBase = URI.create(dst+Path.SEPARATOR);

        List<INode> directories = nodeManager.getDirectories(rootSrcNode);
        for (INode dir : directories) {
          // duplicate each INode in subdirectory
          nodeManager.map(dir, new Function() {
            @Override
            public void apply(INode srcNode) throws IOException {
              String iSrc = srcNode.getRowKey().getPath();
              String iDst = changeBase(iSrc, base, newBase);
              copyWithRenameFlag(srcNode, iDst);
            }
          });
        }
      }
      rootDstNode = copyWithRenameFlag(rootSrcNode, dst);
    }else {
      LOG.debug("Rename Recovery: Skipping Stage 1 because destination " + dst +
          " found");
    }

    // Stage 2: delete old rows
    if(rootSrcNode != null) {
      LOG.debug("Deleting "+src);
      delete(rootSrcNode, true, false, null);
    }else {
      LOG.debug("Rename Recovery: Skipping Stage 2 because "+src+" not found");
    }

    // Stage 3: remove RenameState flags
    if(directoryRename) { // first do Stage 3 for all children
      List<INode> directories = nodeManager.getDirectories(rootDstNode);
      for (INode dir : directories) {
        nodeManager.map(dir, new Function() {
          @Override
          public void apply(INode dstNode) throws IOException {
            removeRenameFlag(dstNode);
          }
        });
      }
    }
    removeRenameFlag(rootDstNode);
  }

  /**
   * Replaces the base prefix of src with newBase.
   */
  private static String changeBase(String src, URI base, URI newBase) {
    URI rel = base.relativize(URI.create(src));
    return newBase.resolve(rel).toString();
  }

  /**
   * Creates a duplicate of srcNode in the namespace named dst and sets the
   * rename flag on the duplicate.
   */
  private INode copyWithRenameFlag(INode srcNode, String dst)
      throws IOException {
    RowKey srcKey = srcNode.getRowKey();
    String src = srcKey.getPath();
    LOG.debug("Copying " + src + " to " + dst + " with rename flag");
    INode dstNode = srcNode.cloneWithNewRowKey(RowKeyFactory.newInstance(dst));
    dstNode.setRenameState(RenameState.TRUE(srcKey.getKey()));
    nodeManager.updateINode(dstNode);
    return dstNode;
  }

  /**
   * Unsets the rename flag from the given node and updates the namespace.
   */
  private void removeRenameFlag(INode dstNode) throws IOException {
    LOG.debug("Removing rename flag from "+dstNode.getRowKey().getPath());
    dstNode.setRenameState(RenameState.FALSE());
    nodeManager.updateINode(dstNode);
  }

  /**
   * Checks the rename arguments and their corresponding INodes to see if this
   * rename can proceed. Derived heavily from {@link
   * org.apache.hadoop.hdfs.server.namenode.FSDirectory#unprotectedRenameTo}
   * @throws IOException thrown if rename cannot proceed
   */
  private void checkCanRename(String src, INode srcNode, String dst,
                              INode dstNode, boolean overwrite) throws
      IOException {
    String error;
    INode dstParentNode = nodeManager.getParentINode(dst);

    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      INode srcParentNode = nodeManager.getParentINode(src);
      pc.check(srcParentNode, FsAction.WRITE_EXECUTE);
      pc.checkStickyBit(srcParentNode, srcNode);
      pc.check(dstParentNode, FsAction.WRITE_EXECUTE);
    }

    boolean src_exists = (srcNode != null);
    boolean dst_exists = (dstNode != null);
    boolean parent_exists = (dstParentNode != null);
    boolean dst_setflag = (dst_exists && dstNode.getRenameState().getFlag());

    // validate source
    if (!src_exists && !dst_setflag) {
      error = "rename source " + src + " is not found.";
      throw new FileNotFoundException(error);
    }
    if (src.equals(Path.SEPARATOR)) {
      error = "rename source cannot be the root";
      throw new IOException(error);
    }

    // validate destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException(
          "The source "+src+" and destination "+dst+" are the same");
    }
    if (dst.startsWith(src) && // dst cannot be a directory or a file under src
        dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      error = "Rename destination " + dst
          + " is a directory or file under source " + src;
      throw new IOException(error);
    }
    if (dst.equals(Path.SEPARATOR)) {
      error = "rename destination cannot be the root";
      throw new IOException(error);
    }
    if (dst_exists && !dst_setflag) { // Destination exists
      if (dstNode.isDir() != srcNode.isDir()) {
        error = "Source " + src + " and destination " + dst
            + " must both be of same kind (file or directory)";
        throw new IOException(error);
      }
      if (!overwrite) { // If destination exists, overwrite flag must be true
        error = "rename destination " + dst + " already exists";
        throw new FileAlreadyExistsException(error);
      }
    }

    // validate parent
    if (!parent_exists) {
      error = "rename destination parent " + dst + " not found.";
      throw new FileNotFoundException(error);
    }
    if (!dstParentNode.isDir()) {
      error = "rename destination parent " + dst + " is a file.";
      throw new ParentNotDirectoryException(error);
    }
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    throw new IOException("renewDelegationToken is not supported");
  }

  @Override // ClientProtocol
  public void renewLease(String clientName) throws AccessControlException,
      IOException {
    Collection<FileLease> leases = leaseManager.renewLease(clientName);
    if(leases == null || leases.isEmpty()) {
      LOG.warn("No leases; did not renew.");
      return;
    }
    // TODO: Bulk put / mutate INodes.
    for(FileLease lease : leases) {
      INode iNode = nodeManager.getINode(lease.getPath());
      if(iNode != null) {
        LOG.info("Renewed lease: " + lease);
        iNode.setLease(lease);
        nodeManager.updateINodeLease(iNode);
      }
    }
  }

  @Override // ClientProtocol
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    throw new IOException("reportBadBlocks is not supported");
  }

  @Override // ClientProtocol
  public boolean restoreFailedStorage(String arg) throws AccessControlException {
    return false;
  }

  @Override // ClientProtocol
  public void saveNamespace() throws AccessControlException, IOException {
    throw new IOException("saveNamespace is not supported");
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    if(username == null && groupname == null)
      return;
    
    INode node = nodeManager.getINode(src);

    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
      pc.checkOwner(node);
      if (!pc.isSuperUser()) {
        if (username != null && !pc.getUser().equals(username)) {
          throw new AccessControlException(
              "Non-super user cannot change owner: " + pc.getUser());
        }
        if (groupname != null && !pc.containsGroup(groupname)) {
          throw new AccessControlException(
              "User does not belong to " + groupname);
        }
      }
    }

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    node.setOwner(username, groupname);
    nodeManager.updateINode(node);
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {

    INode node = nodeManager.getINode(src);

    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
      pc.checkOwner(node);
    }

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    
    node.setPermission(permission);
    nodeManager.updateINode(node);
  }

  @Override // ClientProtocol
  public void setQuota(String src, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    if (isPermissionEnabled) {
      getFsPermissionChecker().checkSuperuserPrivilege();
    }

    INode node = nodeManager.getINode(src);

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    //can only set Quota for directories
    if(!node.isDir()) {
      throw new FileNotFoundException("Directory does not exist: " + src);
    }
    
    // sanity check
    if ((namespaceQuota < 0 && namespaceQuota != HdfsConstants.QUOTA_DONT_SET && 
        namespaceQuota < HdfsConstants.QUOTA_RESET) || 
        (diskspaceQuota < 0 && diskspaceQuota != HdfsConstants.QUOTA_DONT_SET && 
        diskspaceQuota < HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
          "dsQuota : " + namespaceQuota + " and " + diskspaceQuota);
    }

    node.setQuota(namespaceQuota, diskspaceQuota);
    nodeManager.updateINode(node);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    INode node = nodeManager.getINode(src);

    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
      pc.check(node, FsAction.WRITE);
    }

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if(node.isDir())
      return false;

    node.setReplication(replication);
    nodeManager.updateINode(node);
    return true;
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    throw new IOException("setSafeMode is not supported");
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    INode node = nodeManager.getINode(src);

    if (isPermissionEnabled) {
      FSPermissionChecker pc = getFsPermissionChecker();
      pc.check(nodeManager.getParentINode(src), FsAction.EXECUTE);
      pc.check(node, FsAction.WRITE);
    }

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if(node.isDir())
      return;

    node.setTimes(mtime, atime);
    nodeManager.updateINode(node);
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
      throws IOException {
    throw new IOException("updateBlockForPipeline is not supported");
  }

  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
                             ExtendedBlock newBlock, DatanodeID[] newNodes,
                             String[] newStorageIDs)
      throws IOException {
    throw new IOException("updatePipeline is not supported");
  }

  /**
   * Check if file is under construction and if lease matches holder's name.
   * Returns true if INode has no lease or if lease holder matches param
   * holder's name.
   * Returns false otherwise.
   * @param src The path to the INode
   * @param file INode representing file in File System
   * @param holder the client's name who should have lease
   */
  void checkLease(String src, INode file, String holder)
      throws LeaseExpiredException {
    if (file == null || file.isDir()) {
      throw new LeaseExpiredException(
          "No lease on " + src + ": File does not exist. Holder " +
              holder + " does not have any open files.");
    }
    if (!file.getFileState().equals(FileState.UNDER_CONSTRUCTION)) {
      throw new LeaseExpiredException(
          "No lease on " + src + ": File is not open for writing. Holder " +
              holder + " does not have any open files.");
    }
    if(file.getFileState().equals(FileState.UNDER_CONSTRUCTION) &&
        !file.getLease().getHolder().equals(holder)) {
      throw new LeaseExpiredException("Lease mismatch on " +
          src + " owned by " +
          file.getLease().getHolder() + " but is accessed by " + holder);
    }
  }

  /**
   * Try to take over the lease on behalf of param clientName.
   * @param file INode representing a file in the File System
   * @param src The path to the INode
   * @param clientName Name of the client taking the lease
   * @param force Should forcefully take the lease
   */
  void reassignLease(INode file, String src, String clientName,
                     boolean force)
      throws AlreadyBeingCreatedException {
    FileLease lease = file.getLease();
    if (!force && lease != null) {
      if (lease.getHolder().equals(clientName)) {
        throw new AlreadyBeingCreatedException("Failed to create file " + src +
            " for " + clientName + " because current leaseholder is trying to" +
            " recreate file.");
      }
      if(!lease.getHolder().equals(clientName) &&
          !leaseManager.isLeaseSoftLimitExpired(clientName)) {
        throw new AlreadyBeingCreatedException("Failed to create file ["
            + src + "] for [" + clientName + "], because this file is " +
            "already being created by [" + lease.getHolder() + "].");
      }
    }
    file.setLease(new FileLease(clientName, src, now()));
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String src, long fileId,
                                            ExtendedBlock blk,
                                            DatanodeInfo[] existings,
                                            String[] existingStorageIds,
                                            DatanodeInfo[] excludes,
                                            int numAdditionalNodes,
                                            String clientName)
      throws AccessControlException, FileNotFoundException, SafeModeException,
             UnresolvedLinkException, IOException {
    throw new IOException("getAdditionalDatanode is not supported");
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String arg0, String arg1)
      throws IOException {
    throw new IOException("corrupt file block listing is not supported");
  }

  @Override
  public void setBalancerBandwidth(long arg0) throws IOException {
    throw new IOException("bandwidth balancing is not supported");
  }

  @Override
  public long rollEdits() throws AccessControlException, IOException {
    throw new IOException("rollEdits is not supported");
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    throw new IOException("data encryption is not supported");
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    throw new IOException("snapshots are not supported");
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    throw new IOException("snapshots are not supported");
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
                             String snapshotNewName)
      throws IOException {
    throw new IOException("snapshots are not supported");
  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    throw new IOException("snapshots are not supported");
  }

  @Override
  public void disallowSnapshot(String snapshotRoot) throws IOException {
    throw new IOException("snapshots are not supported");
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
                                                  String fromSnapshot,
                                                  String toSnapshot)
      throws IOException {
    throw new IOException("snapshots are not supported");
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo directive,
                                EnumSet<CacheFlag> flags)
      throws IOException {
    throw new IOException("caching is not supported");
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo directive,
                                   EnumSet<CacheFlag> flags)
      throws IOException {
    throw new IOException("caching is not supported");
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    throw new IOException("caching is not supported");
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException {
    throw new IOException("caching is not supported");
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    throw new IOException("caching is not supported");
  }

  @Override
  public void modifyCachePool(CachePoolInfo req) throws IOException {
    throw new IOException("caching is not supported");
  }

  @Override
  public void removeCachePool(String pool) throws IOException {
    throw new IOException("caching is not supported");
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevPool)
      throws IOException {
    throw new IOException("caching is not supported");
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    throw new IOException("ACLs are not supported");
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    throw new IOException("ACLs are not supported");
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    throw new IOException("ACLs are not supported");
  }

  @Override
  public void removeAcl(String src) throws IOException {
    throw new IOException("ACLs are not supported");
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    throw new IOException("ACLs are not supported");
  }

  @Override
  public AclStatus getAclStatus(String src) throws IOException {
    throw new IOException("ACLs are not supported");
  }

  @Override
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    checkXAttrsConfigFlag();
    xAttrOp.setXAttr(src, xAttr, flag);
  }

  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    checkXAttrsConfigFlag();
    return xAttrOp.getXAttrs(src, xAttrs);
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    checkXAttrsConfigFlag();
    return xAttrOp.listXAttrs(src);
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    checkXAttrsConfigFlag();
    xAttrOp.removeXAttr(src, xAttr);
  }

  public boolean internalReleaseLease(FileLease lease, String src)
      throws IOException {
    LOG.info("Recovering " + lease + ", src=" + src);

    try {
      INode iNode = nodeManager.getINode(src);
      if(iNode.getBlocks().size() == 0) {
        LOG.info("Zero blocks detected.");
        return false;
      }
      if(iNode.getFileState().equals(FileState.RECOVERING)) {
        LOG.info("File under recovery already.");
        return false;
      }
      iNode.setState(FileState.RECOVERING);
      nodeManager.updateINode(iNode, BlockAction.RECOVER);
    } catch (IOException e) {
      LOG.error(e);
      return false;
    }
    return true;
  }

  public boolean isRunning() {
    return running;
  }

  public void checkXAttrsConfigFlag() throws IOException {
    if(!this.xattrsEnabled) {
      throw new IOException(String.format("The XAttr operation has been "
       + "rejected.  Support for XAttrs has been disabled by setting %s to"
       + " false.", new Object[]{"dfs.namenode.xattrs.enabled"}));
    }
  }

  private FSPermissionChecker getFsPermissionChecker() throws IOException {
    UserGroupInformation ugi = HBaseRpcUtil.getRemoteUser();
    return new FSPermissionChecker(fsOwnerShortUserName, supergroup, ugi);
  }

  private static void assertNotRoot(String src) {
    assert !new Path(src).isRoot();
  }

}
