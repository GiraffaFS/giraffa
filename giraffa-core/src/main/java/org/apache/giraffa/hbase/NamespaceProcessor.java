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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import org.apache.hadoop.hbase.TableName;
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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FileField;
import org.apache.giraffa.GiraffaConfiguration;
import org.apache.giraffa.GiraffaPBHelper;
import org.apache.giraffa.GiraffaProtos.RenameStateProto;
import org.apache.giraffa.INode;
import org.apache.giraffa.RenameState;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.RowKeyBytes;
import org.apache.giraffa.RowKeyFactory;
import org.apache.giraffa.UnlocatedBlock;
import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.giraffa.hbase.NamespaceAgent.BlockAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
  
  // Coprocessor variables needed
  private RegionCoprocessorEnvironment env;
  private Configuration conf;
  private FsServerDefaults serverDefaults;

  private int lsLimit;

  private final ThreadLocal<HTableInterface> table =
      new ThreadLocal<HTableInterface>() {
          @Override
          public HTableInterface initialValue() {
            return null;
          }
      };

  private static final Log LOG =
      LogFactory.getLog(NamespaceProcessor.class.getName());
   
  static final FsPermission UMASK = FsPermission.createImmutable((short)0111);
  
  public NamespaceProcessor() {}
  
  @Override // CoprocessorService
  public Service getService() {
    return service;
  }

  @Override // Coprocessor
  public void start(CoprocessorEnvironment env) throws IOException {    
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
    
    LOG.info("Start NamespaceProcessor...");
    this.conf = env.getConfiguration();
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
    String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY, DFS_CHECKSUM_TYPE_DEFAULT);
    DataChecksum.Type checksumType;
    try {
       checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
    } catch (IllegalArgumentException iae) {
       throw new IOException("Invalid checksum type in "
          + DFS_CHECKSUM_TYPE_KEY + ": " + checksumTypeStr);
    }
    
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
  }

  @Override // Coprocessor
  public void stop(CoprocessorEnvironment env) {
    LOG.info("Stopping NamespaceProcessor...");
    closeTable();
  }

  HTableInterface getThreadLocalTable() {
    return table.get();
  }

  void setThreadLocalTable(HTableInterface client) {
    table.set(client);
  }

  private void openTable() {
    HTableInterface client = getThreadLocalTable();
    if(client != null)
      return;
    String tableName = conf.get(GiraffaConfiguration.GRFA_TABLE_NAME_KEY,
        GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT);
    try {
      client = env.getTable(TableName.valueOf(tableName));
      setThreadLocalTable(client);
    } catch (IOException e) {
      LOG.error("Cannot get table: " + tableName, e);
    }
  }

  private void closeTable() {
    HTableInterface client = getThreadLocalTable();
    try {
      if(client != null) {
        client.close();
        table.remove();
      }
    } catch (IOException e) {
      LOG.error("Cannot close table: ",e);
    }
  }

  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    throw new IOException("abandonBlock is not supported");
  }

  @Override // ClientProtocol
  public LocatedBlock addBlock(
      String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludeNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    INode iNode = getINode(src, true);

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
    updateINode(iNode, BlockAction.ALLOCATE);
    
    // grab blocks back from HBase and return the latest one added
    Result nodeInfo;
    nodeInfo = getThreadLocalTable().get(new Get(iNode.getRowKey().getKey()));
    List<UnlocatedBlock> al_blks = getBlocks(nodeInfo);
    List<DatanodeInfo[]> al_locs = getLocations(nodeInfo);
    int last = al_blks.size()-1;
    
    if(al_blks.size() != al_locs.size()) {
      throw new IOException("Number of block infos (" + al_blks.size() +
          ") and number of location infos (" + al_locs.size() +
          ") do not match");
    }
    
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
  public boolean complete(String src, String clientName, ExtendedBlock last)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    if(last == null)
      return true;
    INode iNode = getINode(src, true);

    if(iNode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    // set the state and replace the block, then put the iNode
    iNode.setState(FileState.CLOSED);
    iNode.setLastBlock(last);
    long time = now();
    iNode.setTimes(time, time);
    updateINode(iNode, BlockAction.CLOSE);
    LOG.info("Completed file: "+src+" | BlockID: "+last.getBlockId());
    return true;
  }

  @Override // ClientProtocol
  public void concat(String trg, String[] srcs) throws IOException,
      UnresolvedLinkException {
    throw new IOException("concat is not supported");
  }

  @Override // ClientProtocol
  public void create(
      String src, FsPermission masked, String clientName,
      EnumSetWritable<CreateFlag> createFlag, boolean createParent,
      short replication, long blockSize) throws AccessControlException,
      AlreadyBeingCreatedException, DSQuotaExceededException,
      NSQuotaExceededException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    EnumSet<CreateFlag> flag = createFlag.get();
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean append = flag.contains(CreateFlag.APPEND);
    boolean create = flag.contains(CreateFlag.CREATE);

    if(append) {
      throw new IOException("Append is not supported.");
    }

    INode iFile = getINode(src);
    if(create && !overwrite && iFile != null) {
      throw new FileAlreadyExistsException("File already exists: " + src);
    }

    if(iFile != null && iFile.isDir()) {
      throw new FileAlreadyExistsException("File already exists as directory: "
          + src);
    }

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    clientName = ugi.getShortUserName();
    String machineName = (ugi.getGroupNames().length == 0) ? "supergroup" : ugi.getGroupNames()[0];
    masked = new FsPermission((short) 0644);

    Path parentPath = new Path(src).getParent();
    assert parentPath != null : "File must have a parent";
    String parent = parentPath.toString();
    INode iParent = getINode(parent);
    if(!createParent && iParent == null) {
      throw new FileNotFoundException("Parent does not exist: " + src);
    }

    if(iParent == null) { // create parent directories
      if(!mkdirs(parent, masked, true)) {
        throw new IOException("Cannot create parent directories: " + src);
      }
    } else if(!iParent.isDir()) {
      throw new ParentNotDirectoryException("Parent path is not a directory: "
          + src);
    }

    if(overwrite && iFile != null) {
      if(!deleteFile(iFile, true)) {
        throw new IOException("Cannot override existing file: " + src);
      }
    }

    // if file did not exist, create its INode now
    if(iFile == null) {
      RowKey key = RowKeyFactory.newInstance(src);
      long time = now();
      iFile = new INode(0, false, replication, blockSize, time, time,
          masked, clientName, machineName, null,
          key, 0, 0, FileState.UNDER_CONSTRUCTION, null, null, null);
    }

    // add file to HBase (update if already exists)
    updateINode(iFile);
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
    //check parent path first
    Path parentPath = new Path(src).getParent();
    assert parentPath != null : "File must have a parent";

    INode node = getINode(src);
    if(node == null) return false;

    // then check parent inode
    INode parent = getINode(parentPath.toString());
    if(parent == null)
      throw new FileNotFoundException("Parent does not exist.");
    if(!parent.isDir())
      throw new ParentNotDirectoryException("Parent is not a directory.");

    if(node.isDir())
      return deleteDirectory(node, recursive, true);

    return deleteFile(node, true);
  }

  private boolean deleteFile(INode node, boolean deleteBlocks)
      throws IOException {
    if(deleteBlocks) {
      node.setState(FileState.DELETED);
      updateINode(node, BlockAction.DELETE);
    }

    // delete the child key atomically first
    Delete delete = new Delete(node.getRowKey().getKey());
    getThreadLocalTable().delete(delete);

    // delete time penalty (resolves timestamp milliseconds issue)
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      // do nothing
    }

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
                                  boolean deleteBlocks)
      throws AccessControlException, FileNotFoundException, 
      UnresolvedLinkException, IOException {
    if(recursive) {
      List<INode> directories = getDirectoriesRecursive(node);
      // start ascending the tree (breadth first, then depth)
      // we do this by iterating through directories in reverse
      ListIterator<INode> it = directories.listIterator(directories.size());
      while (it.hasPrevious()) {
        INode dir = it.previous();
        RowKey dirRowKey = dir.getRowKey();
        String dirPath = dirRowKey.getPath();
        ResultScanner rs = getListingScanner(dirRowKey,
            HdfsFileStatus.EMPTY_NAME);
        ArrayList<Delete> deletes = new ArrayList<Delete>();
        // schedule immediate children for deletion
        try {
          for (Result result = rs.next(); result != null; result = rs.next()) {
            INode child = newINodeByParent(dirPath, result, true);
            if (!child.isDir())
              deleteFile(child, deleteBlocks);
            else
              deletes.add(new Delete(result.getRow()));
          }
          // perform delete (if non-empty)
          if(!deletes.isEmpty())
            getThreadLocalTable().delete(deletes);
        } finally {
          rs.close();
        }
      }
    } else {
      // check that node has no children
      ResultScanner rs = getListingScanner(node.getRowKey(),
          HdfsFileStatus.EMPTY_NAME);
      try {
        if(rs.next() != null)
          return false;
      } finally {
        rs.close();
      }
    }

    // delete source directory
    Delete delete = new Delete(node.getRowKey().getKey());
    getThreadLocalTable().delete(delete);
    return true;
  }

  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    throw new IOException("upgrade is not supported");
  }

  @Override // ClientProtocol
  public void fsync(String src, String client, long lastBlockLength) throws
      AccessControlException, FileNotFoundException, UnresolvedLinkException,
      IOException {
    throw new IOException("fsync is not supported.");
  }

  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    INode iNode = getINode(src, true);
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
    INode node = getINode(path);
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
    INode node = getINode(src);
    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    return node.getFileStatus();
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    throw new IOException("symlinks are not supported");
  }

  private INode getINode(String path) throws IOException {
    return getINode(path, false);
  }

  /**
   * Fetch an INode by source path String with / without block locations.
   * @param path the source path String
   * @param needLocation whether to get block locations
   * @return INode with / without block locations
   * @throws IOException
   */
  private INode getINode(String path, boolean needLocation) throws IOException {
    return getINode(RowKeyFactory.newInstance(path), needLocation);
  }

  private INode getINode(RowKey key) throws IOException {
    return getINode(key, false);
  }

  /**
   * Fetch an INode, by RowKey, with / without block locations.
   * @param key the RowKey
   * @param needLocation whether to get block locations
   * @return INode with / without block locations
   * @throws IOException
   */
  private INode getINode(RowKey key, boolean needLocation) throws IOException {
    openTable();
    Result nodeInfo;

    nodeInfo = getThreadLocalTable().get(new Get(key.getKey()));
    if(nodeInfo.isEmpty()) {
      LOG.debug("File does not exist: " + key.getPath());
      return null;
    }
    return newINode(key.getPath(), nodeInfo, needLocation);
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
    INode node = getINode(src, needLocation);

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    if(!node.isDir()) {
      return new DirectoryListing(new HdfsFileStatus[] { (needLocation) ?
          node.getLocatedFileStatus() : node.getFileStatus() }, 0);
    }

    List<INode> list = this.getListingInternal(node, startAfter, needLocation);

    HdfsFileStatus[] retVal = new HdfsFileStatus[list.size()];
    int i = 0;
    for(INode child : list)
      retVal[i++] = (needLocation) ? child.getLocatedFileStatus() :
          child.getFileStatus();
    // We can say there is no more entries if the lsLimit is exhausted,
    // otherwise we know only that there could be one more entry
    return new DirectoryListing(retVal, list.size() < lsLimit ? 0 : 1);
  }

  private ResultScanner getListingScanner(RowKey key, byte[] startAfter) 
      throws IOException {
    byte[] start = key.getStartListingKey(startAfter);
    byte[] stop = key.getStopListingKey();
    Scan scan = new Scan(start, stop);
    return getThreadLocalTable().getScanner(scan);
  }

  private List<INode> getListingInternal(
      INode dir, byte[] startAfter, boolean needLocation)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    RowKey key = dir.getRowKey();
    ResultScanner rs = getListingScanner(key, startAfter);
    ArrayList<INode> list = new ArrayList<INode>();
    try {
      for (Result result = rs.next();
           result != null && list.size() < lsLimit;
           result = rs.next()) {
        list.add(newINodeByParent(key.getPath(), result, needLocation));
      }
    } finally {
      rs.close();
    }
    return list;
  }

  private List<INode> getDirectoriesRecursive(INode root) throws IOException {
    List<INode> directories = new ArrayList<INode>();
    directories.add(root);

    // start loop descending the tree (breadth first, then depth)
    for(int i = 0; i < directories.size(); i++) {
      // get next directory INode in the list and it's Scanner
      INode dir = directories.get(i);
      RowKey key = dir.getRowKey();
      ResultScanner rs = getListingScanner(key, HdfsFileStatus.EMPTY_NAME);
      try {
        for (Result result = rs.next(); result != null; result = rs.next()) {
          if (getDirectory(result)) {
            directories.add(newINodeByParent(key.getPath(), result, false));
          }
        }
      } finally {
        rs.close();
      }
    }

    return directories;
  }

  @Override // ClientProtocol
  public long getPreferredBlockSize(String src) throws IOException,
      UnresolvedLinkException {
    INode inode = getINode(src);
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
    Path parentPath = new Path(src).getParent();
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    String clientName = ugi.getShortUserName();
    String machineName = (ugi.getGroupNames().length == 0) ? "supergroup" : ugi.getGroupNames()[0];

    RowKey key = RowKeyFactory.newInstance(src);
    INode inode = getINode(key);
    if(parentPath == null) {
      //generate root if doesn't exist
      if(inode == null) {
        long time = now();
        inode = new INode(0, true, (short) 0, 0, time, time,
            masked, clientName, machineName, null,
            key, 0, 0, null, null, null, null);
        updateINode(inode);
      }
      return true;
    }

    if(inode != null) {  // already exists
      return true;
    }

    // create parent directories if requested
    String parent = parentPath.toString();
    INode iParent = getINode(parent);
    if(!createParent && iParent == null) {
      throw new FileNotFoundException("Parent does not exist: "+parent);
    }
    if(iParent != null && !iParent.isDir()) {
      throw new ParentNotDirectoryException("Parent is not directory: "+parent);
    }
    if(createParent && iParent == null) {
      //make the parent directories
      mkdirs(parent, masked, true);
    } 

    long time = now();
    inode = new INode(0, true, (short) 0, 0, time, time,
        masked, clientName, machineName, null,
        key, 0, 0, null, null, null, null);

    // add directory to HBase
    updateINode(inode);
    return true;
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName) throws IOException {
    throw new IOException("recoverLease is not supported");
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
    }catch(IOException e){
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
    ResultScanner rs;
    INode rootSrcNode = getINode(src, true);
    INode rootDstNode = getINode(dst, true);
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
        URI base = new Path(src).toUri();
        URI newBase = URI.create(dst+Path.SEPARATOR);

        List<INode> directories = getDirectoriesRecursive(rootSrcNode);
        for (INode dir : directories) {
          RowKey key = dir.getRowKey();
          String parent = key.getPath();
          // duplicate each INode in subdirectory
          rs = getListingScanner(key, HdfsFileStatus.EMPTY_NAME);
          try {
            for (Result res = rs.next(); res != null; res = rs.next()) {
              INode srcNode = newINodeByParent(parent, res, true);
              String iSrc = srcNode.getRowKey().getPath();
              String iDst = changeBase(iSrc, base, newBase);
              copyWithRenameFlag(srcNode, iDst);
            }
          } finally {
            rs.close();
          }
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
      if(directoryRename)
        deleteDirectory(rootSrcNode, true, false);
      else
        deleteFile(rootSrcNode, false);
    }else {
      LOG.debug("Rename Recovery: Skipping Stage 2 because "+src+" not found");
    }

    // Stage 3: remove RenameState flags
    if(directoryRename) { // first do Stage 3 for all children
      List<INode> directories = getDirectoriesRecursive(rootDstNode);
      for (INode dir : directories) {
        String parent = dir.getRowKey().getPath();
        rs = getListingScanner(dir.getRowKey(), HdfsFileStatus.EMPTY_NAME);
        try {
          for (Result res = rs.next(); res != null; res = rs.next()) {
            removeRenameFlag(newINodeByParent(parent, res, true));
          }
        } finally {
          rs.close();
        }
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
    LOG.debug("Copying "+src+" to "+dst+" with rename flag");
    INode dstNode = srcNode.cloneWithNewRowKey(RowKeyFactory.newInstance(dst));
    dstNode.setRenameState(RenameState.TRUE(srcKey.getKey()));
    updateINode(dstNode);
    return dstNode;
  }

  /**
   * Unsets the rename flag from the given node and updates the namespace.
   */
  private void removeRenameFlag(INode dstNode) throws IOException {
    LOG.debug("Removing rename flag from "+dstNode.getRowKey().getPath());
    dstNode.setRenameState(RenameState.FALSE());
    updateINode(dstNode);
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
    String error = null;
    String parent = new Path(dst).getParent().toString();
    INode parentNode = getINode(parent, false);

    boolean src_exists = (srcNode != null);
    boolean dst_exists = (dstNode != null);
    boolean parent_exists = (parentNode != null);
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
    if (!parentNode.isDir()) {
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
    throw new IOException("renewLease is not supported");
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
    
    INode node = getINode(src);

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    node.setOwner(username, groupname);
    updateINode(node);
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {

    INode node = getINode(src);

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    
    if(!node.isDir()) {
      permission = permission.applyUMask(UMASK);
    }
    
    node.setPermission(permission);
    updateINode(node);
  }

  @Override // ClientProtocol
  public void setQuota(String src, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    
    INode node = getINode(src);

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
    updateINode(node);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    INode node = getINode(src);

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if(node.isDir())
      return false;

    node.setReplication(replication);
    updateINode(node);
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
    INode node = getINode(src);

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if(node.isDir())
      return;

    node.setTimes(mtime, atime);
    updateINode(node);
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
      throws IOException {
    throw new IOException("updateBlockForPipeline is not supported");
  }

  @Override // ClientProtocol
  public void updatePipeline(
      String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock, DatanodeID[] newNodes)
      throws IOException {
    throw new IOException("updatePipeline is not supported");
  }

  /**
   * Fetch an INode by parent path, child result, with / without block locations.
   * @param parent parent source path
   * @param result Result row obtained from HBase by RowKey
   * @param needLocation whether to grab block locations
   * @return fully-constructed INode with / without block locations
   * @throws IOException
   */
  private INode newINodeByParent(String parent, Result result, boolean needLocation)
      throws IOException {
    String cur = new Path(parent, getFileName(result)).toString();
    return newINode(cur, result, needLocation);
  }

  /**
   * This private method is ultimately responsible for generating INode objects based
   * on HBase rows and RowKeys.
   * @param src source path
   * @param result HBase row obtained by RowKey
 * @param needLocation 
   * @return fully constructed INode
   * @throws IOException
   */
  private INode newINode(String src, Result result, boolean needLocation) throws IOException {
    RowKey key = RowKeyFactory.newInstance(src, result.getRow());
    INode iNode = new INode(
        getLength(result),
        getDirectory(result),
        getReplication(result),
        getBlockSize(result),
        getMTime(result),
        getATime(result),
        getPermissions(result),
        getUserName(result),
        getGroupName(result),
        getSymlink(result),
        key,
        getDsQuota(result),
        getNsQuota(result),
        getFileState(result),
        getRenameState(result),
        (needLocation) ? getBlocks(result) : null,
        (needLocation) ? getLocations(result) : null);
    return iNode;
  }

  private void updateINode(INode node) throws IOException {
    updateINode(node, null);
  }

  private void updateINode(INode node, BlockAction ba)
      throws IOException {
    long ts = now();
    RowKey key = node.getRowKey();
    Put put = new Put(key.getKey(), ts);
    put.add(FileField.getFileAttributes(), FileField.getFileName(), ts,
            RowKeyBytes.toBytes(new Path(key.getPath()).getName()))
        .add(FileField.getFileAttributes(), FileField.getUserName(), ts,
            RowKeyBytes.toBytes(node.getOwner()))
        .add(FileField.getFileAttributes(), FileField.getGroupName(), ts,
            RowKeyBytes.toBytes(node.getGroup()))
        .add(FileField.getFileAttributes(), FileField.getLength(), ts,
            Bytes.toBytes(node.getLen()))
        .add(FileField.getFileAttributes(), FileField.getPermissions(), ts,
            Bytes.toBytes(node.getPermission().toShort()))
        .add(FileField.getFileAttributes(), FileField.getMTime(), ts,
            Bytes.toBytes(node.getModificationTime()))
        .add(FileField.getFileAttributes(), FileField.getATime(), ts,
            Bytes.toBytes(node.getAccessTime()))
        .add(FileField.getFileAttributes(), FileField.getDsQuota(), ts,
            Bytes.toBytes(node.getDsQuota()))
        .add(FileField.getFileAttributes(), FileField.getNsQuota(), ts,
            Bytes.toBytes(node.getNsQuota()))
        .add(FileField.getFileAttributes(), FileField.getReplication(), ts,
            Bytes.toBytes(node.getReplication()))
        .add(FileField.getFileAttributes(), FileField.getBlockSize(), ts,
            Bytes.toBytes(node.getBlockSize()))
        .add(FileField.getFileAttributes(), FileField.getRenameState(), ts,
            node.getRenameStateBytes());

    if(node.getSymlink() != null)
      put.add(FileField.getFileAttributes(), FileField.getSymlink(), ts,
          node.getSymlink());

    if(node.isDir())
      put.add(FileField.getFileAttributes(), FileField.getDirectory(), ts,
          Bytes.toBytes(node.isDir()));
    else
      put.add(FileField.getFileAttributes(), FileField.getBlock(), ts,
             node.getBlocksBytes())
         .add(FileField.getFileAttributes(), FileField.getLocations(), ts,
             node.getLocationsBytes())
         .add(FileField.getFileAttributes(), FileField.getFileState(), ts,
             Bytes.toBytes(node.getFileState().toString()));

    if(ba != null)
      put.add(FileField.getFileAttributes(), FileField.getAction(), ts,
          Bytes.toBytes(ba.toString()));

    getThreadLocalTable().put(put);
  }

  /**
   * Get UnlocatedBlock info from HBase based on this nodes internal RowKey.
   * @param res
   * @return UnlocatedBlock from HBase row. Null if a directory or
   *  any sort of Exception happens.
   * @throws IOException
   */
  public static List<UnlocatedBlock> getBlocks(Result res) throws
       IOException {
    if(getDirectory(res))
      return null;
    
    byte[] value = res.getValue(
        FileField.getFileAttributes(), FileField.getBlock());
    return GiraffaPBHelper.bytesToUnlocatedBlocks(value);
  }
  
  public static List<DatanodeInfo[]> getLocations(Result res) throws
      IOException {
    if(getDirectory(res))
      return null;
    
    byte[] value = res.getValue(
        FileField.getFileAttributes(), FileField.getLocations());
    return GiraffaPBHelper.bytesToBlockLocations(value);
  }
  
  public static List<LocatedBlock> getLocatedBlocks(Result res) throws
      IOException {
    return UnlocatedBlock.toLocatedBlocks(getBlocks(res), getLocations(res));
  }

  public static boolean getDirectory(Result res) {
    return res.containsColumn(FileField.getFileAttributes(), FileField.getDirectory());
  }

  public static short getReplication(Result res) {
    return Bytes.toShort(res.getValue(FileField.getFileAttributes(), FileField.getReplication()));
  }

  public static long getBlockSize(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getBlockSize()));
  }

  public static long getMTime(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getMTime()));
  }

  public static long getATime(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getATime()));
  }

  public static FsPermission getPermissions(Result res) {
    return new FsPermission(
        Bytes.toShort(res.getValue(FileField.getFileAttributes(), FileField.getPermissions())));
  }

  public static String getFileName(Result res) {
    return Bytes.toString(res.getValue(FileField.getFileAttributes(),
                                   FileField.getFileName()));
  }

  public static String getUserName(Result res) {
    return Bytes.toString(res.getValue(FileField.getFileAttributes(),
        FileField.getUserName()));
  }

  public static String getGroupName(Result res) {
    return RowKeyBytes.toString(res.getValue(FileField.getFileAttributes(),
        FileField.getGroupName()));
  }

  public static byte[] getSymlink(Result res) {
    return res.getValue(FileField.getFileAttributes(), FileField.getSymlink());
  }

  public static FileState getFileState(Result res) {
    if(getDirectory(res))
      return null;
    return FileState.valueOf(
        Bytes.toString(res.getValue(FileField.getFileAttributes(),
            FileField.getFileState())));
  }

  public static RenameState getRenameState(Result res)
      throws IOException {
    return GiraffaPBHelper.convert(RenameStateProto.parseFrom(
        res.getValue(FileField.getFileAttributes(), FileField.getRenameState())));
  }

  public static long getNsQuota(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getNsQuota()));
  }

  public static long getDsQuota(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getDsQuota()));
  }

  // Get file fields from Result
  public static long getLength(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getLength()));
  }
 
  private static long now() {
    return System.currentTimeMillis();
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String arg0, ExtendedBlock arg1,
      DatanodeInfo[] arg2, DatanodeInfo[] arg3, int arg4, String arg5)
      throws AccessControlException, FileNotFoundException,
      SafeModeException, UnresolvedLinkException, IOException {
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
}
