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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FileField;
import org.apache.giraffa.GiraffaConfiguration;
import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.giraffa.INode;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.hbase.NamespaceAgent.BlockAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

 /**
  */
public class NamespaceProcessor extends BaseEndpointCoprocessor
implements NamespaceProtocol {

  private Class<? extends RowKey> rowKeyClass;
  private boolean caching;

  // private HRegion region;
  private HTableInterface table;

  private HashMap<String, RowKey> cache = new HashMap<String, RowKey>();
  private int lsLimit;

  private static final Log LOG =
    LogFactory.getLog(NamespaceProcessor.class.getName());

  @Override  // BaseEndpointCoprocessor
  public void start(CoprocessorEnvironment env) {
    super.start(env);
    LOG.info("Start NamespaceProcessor...");
    Configuration conf = env.getConfiguration();
    rowKeyClass = conf.getClass(GiraffaConfiguration.GRFA_ROW_KEY_KEY,
                                GiraffaConfiguration.GRFA_ROW_KEY_DEFAULT,
                                RowKey.class);
    caching = conf.getBoolean(GiraffaConfiguration.GRFA_CACHING_KEY,
                              GiraffaConfiguration.GRFA_CACHING_DEFAULT);
    int configuredLimit = conf.getInt(
        GiraffaConfiguration.GRFA_LIST_LIMIT_KEY,
        GiraffaConfiguration.GRFA_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit > 0 ?
        configuredLimit : GiraffaConfiguration.GRFA_LIST_LIMIT_DEFAULT;
    LOG.info("Caching is set to: " + caching);
    LOG.info("RowKey is set to: " + rowKeyClass.getCanonicalName());

    // this.region = ((RegionCoprocessorEnvironment)getEnvironment()).getRegion();
  }

  @Override  // BaseEndpointCoprocessor
  public void stop(CoprocessorEnvironment env) {
    LOG.info("Stopping NamespaceProcessor...");
    super.stop(env);
    try {
      if(table != null) table.close();
      table = null;
    } catch (IOException e) {
      LOG.error("Cannot close table: ",e);
    }
  }

  private void openTable() {
    if(this.table != null)
      return;
    Configuration conf = getEnvironment().getConfiguration();
    String tableName = conf.get(GiraffaConfiguration.GRFA_TABLE_NAME_KEY,
        GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT);
    try {
      table = ((RegionCoprocessorEnvironment)getEnvironment()).getTable(
          tableName.getBytes());
    } catch (IOException e) {
      LOG.error("Cannot get table: " + table, e);
    }
  }

  @Override // ClientProtocol
  public void abandonBlock(Block b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {

  }

  @Override // ClientProtocol
  public LocatedBlock addBlock(
      String src, String clientName, Block previous, DatanodeInfo[] excludeNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    INode iNode = getINode(src);

    if(iNode == null) {
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return null; // HBase RPC does not pass exceptions
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
    Result nodeInfo = table.get(new Get(iNode.getRowKey().getKey()));
    ArrayList<LocatedBlock> al = getBlocks(nodeInfo);
    LOG.info("Added block. File: " + src + " has " + al.size() + " block(s).");
    return al.get(al.size()-1);
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
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName, Block last)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    if(last == null)
      return true;
    INode iNode = getINode(src);

    if(iNode == null) {
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return false; // HBase RPC does not pass exceptions
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
      LOG.error("Append is not supported.");
      // throw IOException("Append is not supported.")
      return;
    }

    INode iFile = getINode(src);
    if(create && iFile != null) {
      LOG.info("File already exists: " + src);
      // throw FileAlreadyExistsException
      return; // HBase RPC does not pass exceptions
    }

    if(iFile != null && iFile.isDir()) {
      LOG.error("File already exists as a directory: " + src);
      // throw FileAlreadyExistsException
      return; // HBase RPC does not pass exceptions
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
      // throw new FileNotFoundException("Parent does not exist: " + src);
      LOG.error("Parent does not exist: " + src);
      return; // HBase RPC does not pass exceptions
    }

    if(iParent == null) { // create parent directories
      if(! mkdirs(parent, masked, true)) {
        LOG.error("Cannot create parent directories: " + src);
        return;
      }
    } else if(!iParent.isDir()) {
      // throw new ParentNotDirectoryException(
      //     "Parent path is not a directory: " + src);
      LOG.error("Parent path is not a directory: " + src);
      return; // HBase RPC does not pass exceptions
    }

    if(overwrite && iFile != null) {
      if(! deleteFile(iFile)) {
        LOG.error("Cannot override existing file: " + src);
        return;
      }
    }

    // if file did not exist, create its INode now
    if(iFile == null) {
      RowKey key = getRowKey(src);
      long time = now();
      iFile = new INode(0, false, replication, blockSize, time, time,
          masked, clientName, machineName, null,
          key, 0, 0, FileState.UNDER_CONSTRUCTION, null);
    }

    // add file to HBase (update if already exists)
    updateINode(iFile);
  }

  /**
   * Method designed to generate a single RowKey. It may try to grab the key
   * from a memory cache.
   * 
   * @param src Used to generate the RowKey
   * @return the RowKey
   * @throws IOException
   */
  private RowKey getRowKey(String src) throws IOException {
    return getRowKey(src, null);
  }

  private RowKey getRowKey(String src, byte[] bytes) throws IOException {
    // try to grab child from cache
    RowKey key = (caching) ? cache.get(src) : null;

    if(key != null) {
      return key;
    }

    // generate new key (throw exception if not possible)
    key = createRowKey(src, bytes);

    return key;
  }

  /**
   * Should only be called in the event that a new RowKey needs to be generated
   * due to create() or mkdirs() file not already existing; this is where the key
   * is cached as well.
   * @param src
   * @return a new RowKey initialized with src
   * @throws IOException 
   */
  private RowKey createRowKey(String src, byte[] bytes) throws IOException {
    RowKey key = ReflectionUtils.newInstance(rowKeyClass, null);
    if(bytes == null)
      key.setPath(src);
    else
      key.set(src, bytes);

    if(caching)
      cache.put(src, key);

    return key;
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
   * Guarantees to atomically delete the source file first, and any subsequent
   * files recursively if desired.
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
      // throw new FileNotFoundException("Parent does not exist.");
      return false; // parent already deleted
    if(!parent.isDir())
      // throw new ParentNotDirectoryException("Parent is not a directory.");
      return false; // parent already replaced

    if(node.isDir())
      return deleteDirectory(node, recursive);

    return deleteFile(node);
  }

  private boolean deleteFile(INode node) throws IOException {
    // delete single file
    node.setState(FileState.DELETED);
    updateINode(node, BlockAction.DELETE);

    // delete the child key atomically first
    Delete delete = new Delete(node.getRowKey().getKey());
    table.delete(delete);

    // delete time penalty (resolves timestamp milliseconds issue)
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      // do nothing
    }

    return true;
  }

  /** 
   * The recursive function which first deletes all children within a directory
   * then the directory itself.
   * If any of the children cannot be deleted the directory itself will not 
   * be deleted as well.
   *
   * @param node
   * @param recursive
   * @return true if the directory was actually deleted
   * @throws AccessControlException
   * @throws FileNotFoundException
   * @throws UnresolvedLinkException
   * @throws IOException
   */
  private boolean deleteDirectory(INode node, boolean recursive) 
      throws AccessControlException, FileNotFoundException, 
      UnresolvedLinkException, IOException {
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    List<INode> children = 
        getListingInternal(node, HdfsFileStatus.EMPTY_NAME, false);
    if(!recursive && children.size() > 0)
        return false;

    for(INode child : children) {
      if(child.isDir()) {
        if(! deleteDirectory(child, true))
          return false;
      } else  // add this key to batch delete
        deletes.add(new Delete(child.getRowKey().getKey()));
    }

    // delete files
    if(!deletes.isEmpty())
      table.delete(deletes);

    // delete current directory
    Delete delete = new Delete(node.getRowKey().getKey());
    table.delete(delete);
    return true;
  }

  @Override // ClientProtocol
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    throw new IOException("distributed upgrade is not supported");
  }

  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    throw new IOException("upgrade is not supported");
  }

  @Override // ClientProtocol
  public void fsync(String src, String client) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    throw new IOException("fsync is not supported.");
  }

  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    INode iNode = getINode(src);
    if(iNode == null || iNode.isDir()) {
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return null; // HBase RPC does not pass exceptions
    }

    List<LocatedBlock> al = iNode.getBlocks();
    boolean underConstruction = 
        (iNode.getFileState().equals(FileState.CLOSED)) ? true : false;

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
    if(node.isDir()) {
      return new ContentSummary(0L, 0L, 1L, node.getNsQuota(), 
          0L, node.getDsQuota());
    }
    return null;
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    throw new IOException("getDatanodeReport is not supported");
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return null;
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    INode node = getINode(src);
    if(node == null) {
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return null; // HBase RPC does not pass exceptions
    }
    return node.getFileStatus();
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    throw new IOException("symlinks are not supported");
  }

  private INode getINode(String path) throws IOException {
    return getINode(getRowKey(path));
  }

  private INode getINode(RowKey key) throws IOException {
    openTable();
    Result nodeInfo = table.get(new Get(key.getKey()));
    if(nodeInfo.isEmpty()) {
      LOG.debug("File does not exist: " + key.getPath());
      return null;
    }
    return newINode(key.getPath(), nodeInfo);
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
    INode node = getINode(src);

    if(node == null) {
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return null; // HBase RPC does not pass exceptions
    }

    if(!node.isDir()) {
      return new DirectoryListing(new HdfsFileStatus[] { node.getFileStatus() }, 0);
    }

    List<INode> list = this.getListingInternal(node, startAfter, needLocation);

    HdfsFileStatus[] retVal = new HdfsFileStatus[list.size()];
    int i = 0;
    for(INode child : list)
      retVal[i++] = child.getFileStatus();
    // We can say there is no more entries if the lsLimit is exhausted,
    // otherwise we know only that there could be one more entry
    return new DirectoryListing(retVal, list.size() < lsLimit ? 0 : 1);
  }

  private List<INode> getListingInternal(
      INode dir, byte[] startAfter, boolean needLocation)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    RowKey key = dir.getRowKey();
    byte[] start = key.getStartListingKey(startAfter);
    byte[] stop = key.getStopListingKey();
    Scan scan = new Scan(start, stop);
    ResultScanner rs = table.getScanner(scan);

    ArrayList<INode> list = new ArrayList<INode>();
    for(Result result = rs.next();
        result != null && list.size() < lsLimit;
        result = rs.next()) {
      list.add(newINodeByParent(key.getPath(), result));
    }

    return list;
  }

  @Override // ClientProtocol
  public long getPreferredBlockSize(String src) throws IOException,
      UnresolvedLinkException {
    INode inode = getINode(src);
    if(inode == null) {
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return -1; // HBase RPC does not pass exceptions
    }
    return inode.getBlockSize();
  }

  @Override // ClientProtocol
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(ClientProtocol.class.getName()))
      return ClientProtocol.versionID;
    else
      throw new IOException("Unknown protocol: " + protocol);
  }

  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    throw new IOException("getServerDefaults is not supported");
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

    RowKey key = getRowKey(src);
    INode inode = getINode(key);
    if(parentPath == null) {
      //generate root if doesn't exist
      if(inode == null) {
        long time = now();
        inode = new INode(0, true, (short) 0, 0, time, time,
            masked, clientName, machineName, null,
            key, 0, 0, null, null);
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
      // throw new FileNotFoundException();
      return false;
    }
    if(iParent != null && !iParent.isDir()) {
      // throw new ParentNotDirectoryException();
      return false;
    }
    if(createParent && iParent == null) {
      //make the parent directories
      mkdirs(parent, masked, true);
    } 

    long time = now();
    inode = new INode(0, true, (short) 0, 0, time, time,
        masked, clientName, machineName, null,
        key, 0, 0, null, null);

    // add directory to HBase
    updateINode(inode);
    return true;
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName) throws IOException {
    return false;
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    throw new IOException("refreshNodes is not supported");
  }

  @Override // ClientProtocol
  public boolean rename(String src, String dst) throws UnresolvedLinkException,
      IOException {
    throw new IOException("rename is not supported");
  }

  @Override // ClientProtocol
  public void rename(String src, String dst, Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    throw new IOException("rename is not supported");
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    return 0;
  }

  @Override // ClientProtocol
  public void renewLease(String clientName) throws AccessControlException,
      IOException {
  }

  @Override // ClientProtocol
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
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
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return; // HBase RPC does not pass exceptions
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
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return; // HBase RPC does not pass exceptions
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
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return; // HBase RPC does not pass exceptions
    }

    //can only set Quota for directories
    if(!node.isDir())
      return;

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
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return false; // HBase RPC does not pass exceptions
    }
    if(node.isDir())
      return false;

    node.setReplication(replication);
    updateINode(node);
    return true;
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return false;
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    INode node = getINode(src);

    if(node == null) {
      // throw new FileNotFoundException("File does not exist: " + src);
      LOG.error("File does not exist: " + src);
      return; // HBase RPC does not pass exceptions
    }
    if(node.isDir())
      return;

    node.setTimes(mtime, atime);
    updateINode(node);
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(Block block, String clientName)
      throws IOException {
    return null;
  }

  @Override // ClientProtocol
  public void updatePipeline(
      String clientName, Block oldBlock, Block newBlock, DatanodeID[] newNodes)
      throws IOException {
  }

  private INode newINodeByParent(String parent, Result result)
      throws IOException {
    String cur = new Path(parent, getFileName(result)).toString();
    return newINode(cur, result);
  }

  private INode newINode(String src, Result result) throws IOException {
    RowKey key = getRowKey(src, result.getRow());
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
        getState(result),
        getBlocks(result));
    return iNode;
  }

  private void updateINode(INode node) throws IOException {
    updateINode(node, null);
  }

  private void updateINode(INode node, BlockAction ba) throws IOException {
    long ts = now();
    RowKey key = node.getRowKey();
    Put put = new Put(key.getKey(), ts);
    put.add(FileField.getFileAttributes(), FileField.getFileName(), ts,
            new Path(key.getPath()).getName().getBytes())
        .add(FileField.getFileAttributes(), FileField.getUserName(), ts,
            node.getOwner().getBytes())
        .add(FileField.getFileAttributes(), FileField.getGroupName(), ts,
            node.getGroup().getBytes())
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
            Bytes.toBytes(node.getBlockSize()));

    if(node.getSymlink() != null)
      put.add(FileField.getFileAttributes(), FileField.getSymlink(), ts,
          node.getSymlink());

    if(node.isDir())
      put.add(FileField.getFileAttributes(), FileField.getDirectory(), ts,
          Bytes.toBytes(node.isDir()));
    else
      put.add(FileField.getFileAttributes(), FileField.getBlock(), ts,
             node.getBlocksBytes())
         .add(FileField.getFileAttributes(), FileField.getState(), ts,
             Bytes.toBytes(node.getFileState().toString()));

    if(ba != null)
      put.add(FileField.getFileAttributes(), FileField.getAction(), ts,
          Bytes.toBytes(ba.toString()));

    table.put(put);
  }

  /**
   * Get LocatedBlock info from HBase based on this nodes internal RowKey.
   * @param res
   * @return LocatedBlock from HBase row. Null if a directory or
   *  any sort of Exception happens.
   * @throws IOException
   */
  static ArrayList<LocatedBlock> getBlocks(Result res) throws IOException {
    if(getDirectory(res))
      return null;
  
    ArrayList<LocatedBlock> blocks = new ArrayList<LocatedBlock>();
    DataInputStream in = null;
    byte[] value = res.getValue(
        FileField.getFileAttributes(), FileField.getBlock());
    in = new DataInputStream(new ByteArrayInputStream(value));
    while(in.available() > 0) {
      LocatedBlock loc = LocatedBlock.read(in);
      blocks.add(loc);
    }
    in.close();
    return blocks;
  }

  /**
   * Get DirectoryTable from HBase.
   * 
   * @param res
   * @return The directory table.
  static DirectoryTable getDirectoryTable(Result res) {
    if(!getDirectory(res))
      return null;

    DirectoryTable dirTable;
    try {
      dirTable = new DirectoryTable(res.getValue(
          FileField.getFileAttributes(), FileField.getDirectory()));
    } catch (IOException e) {
      INode.LOG.info("Cannot get directory table", e);
      return null;
    } catch (ClassNotFoundException e) {
      INode.LOG.info("Cannot get directory table", e);
      return null;
    }
    return dirTable;
  }
   */

  static boolean getDirectory(Result res) {
    return res.containsColumn(FileField.getFileAttributes(), FileField.getDirectory());
  }

  static short getReplication(Result res) {
    return Bytes.toShort(res.getValue(FileField.getFileAttributes(), FileField.getReplication()));
  }

  static long getBlockSize(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getBlockSize()));
  }

  static long getMTime(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getMTime()));
  }

  static long getATime(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getATime()));
  }

  static FsPermission getPermissions(Result res) {
    return new FsPermission(
        Bytes.toShort(res.getValue(FileField.getFileAttributes(), FileField.getPermissions())));
  }

  static String getFileName(Result res) {
    return new String(res.getValue(FileField.getFileAttributes(),
                                   FileField.getFileName()));
  }

  static String getUserName(Result res) {
    return new String(res.getValue(FileField.getFileAttributes(), FileField.getUserName()));
  }

  static String getGroupName(Result res) {
    return new String(res.getValue(FileField.getFileAttributes(), FileField.getGroupName()));
  }

  static byte[] getSymlink(Result res) {
    return res.getValue(FileField.getFileAttributes(), FileField.getSymlink());
  }

  static FileState getState(Result res) {
    if(getDirectory(res))
      return null;
    return FileState.valueOf(
        Bytes.toString(res.getValue(FileField.getFileAttributes(), FileField.getState())));
  }

  static long getNsQuota(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getNsQuota()));
  }

  static long getDsQuota(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getDsQuota()));
  }

  // Get file fields from Result
  static long getLength(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(), FileField.getLength()));
  }
}
