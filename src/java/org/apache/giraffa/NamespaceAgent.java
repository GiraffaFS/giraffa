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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

 /**
  * NamespaceAgent is the proxy used by DFSClient to communicate with HBase
  * as if it is a NameNode.
  * NamespaceAgent implements ClientProtocol and is a replacement of the 
  * NameNode RPC proxy.
  */
public class NamespaceAgent implements ClientProtocol {

  private final Class<? extends RowKey> rowKeyClass;
  private final boolean caching;

  private HBaseAdmin hbAdmin;
  private HTable nsTable;

  private HashMap<String, RowKey> cache = new HashMap<String, RowKey>();

  private static final Log LOG =
    LogFactory.getLog(NamespaceAgent.class.getName());

  @SuppressWarnings("unchecked")
  public NamespaceAgent(GiraffaConfiguration conf) throws IOException,
    ClassNotFoundException {
    rowKeyClass = (Class<? extends RowKey>) Class.forName(
        conf.get(GiraffaConfiguration.GRFA_ROW_KEY_KEY,
                 GiraffaConfiguration.GRFA_ROW_KEY_DEFAULT));
    caching = conf.getBoolean(GiraffaConfiguration.GRFA_CACHING_KEY,
                              GiraffaConfiguration.GRFA_CACHING_DEFAULT);
    LOG.info("Caching is set to: " + caching);
    LOG.info("RowKey is set to: " + rowKeyClass.getCanonicalName());
    this.hbAdmin = new HBaseAdmin(HBaseConfiguration.create(conf));
    try {
      this.nsTable = new HTable(hbAdmin.getConfiguration(),
          conf.get(GiraffaConfiguration.GRFA_TABLE_NAME_KEY,
              GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT));
    } catch(TableNotFoundException tnfe) {
      throw new IOException("Giraffa is not formatted.", tnfe);
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
    Path path = new Path(src);
    INode iNode = getINode(path);

    if(iNode == null)
      throw new FileNotFoundException();

    // Calls addBlock on HDFS by putting another empty Block in HBase
    Result nodeInfo = nsTable.get(new Get(iNode.getRowKey().getKey()));
    // we need to grab blocks from HBase and then add to it
    iNode.getBlocks(nodeInfo);
    if(previous != null) {
      // we need to update in HBase the previous block
      iNode.replaceBlock(previous);
    }

    // add a Block and modify times
    // (if there was a previous block this call with add it in as well)
    iNode.addBlock(nsTable);
    long time = now();
    iNode.updateTimes(nsTable, time, time);

    // grab blocks back from HBase and return the latest one added
    nodeInfo = nsTable.get(new Get(iNode.getRowKey().getKey()));
    ArrayList<LocatedBlock> al = iNode.getBlocks(nodeInfo);
    LOG.info("Added block. File: " + src + " has " + al.size() + " block(s).");
    return al.get(al.size()-1);
  }

  @Override // ClientProtocol
  public LocatedBlock append(String src, String clientName)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    throw new IOException("append not supported");
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
    Path path = new Path(src);
    INode iNode = getINode(path);

    if(iNode == null) {
      throw new FileNotFoundException();
    }

    Result nodeInfo = nsTable.get(new Get(iNode.getRowKey().getKey()));
    // set the state and replace the block, then put the iNode
    iNode.setState(FileState.CLOSED);
    iNode.getBlocks(nodeInfo);
    iNode.replaceBlock(last);
    iNode.putINode(nsTable);
    long time = now();
    iNode.updateTimes(nsTable, time, time);
    LOG.info("Completed file: "+src+" | BlockID: "+last.getBlockId());
    return true;
  }

  @Override // ClientProtocol
  public void concat(String trg, String[] srcs) throws IOException,
      UnresolvedLinkException {
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
    boolean updateParent = true;

    Path path = new Path(src);
    if(path.getParent() == null) {
      throw new IOException("Root has no parent.");
    }

    INode iParent = getINode(path.getParent());
    INode iFile = getINode(path);

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    String machineName = (ugi.getGroupNames().length == 0) ? "" : ugi.getGroupNames()[0];

    if(!createParent && iParent == null) {
      // SHV !!! make sure parent exists and is a directory
      throw new FileNotFoundException();
    } else if(createParent && iParent == null) {
      // make the parent directories
      mkdirs(path.getParent().toString(), masked, true);
    }

    if(!overwrite && iFile != null) {
      // SHV !!! make sure file does not exist
      throw new FileAlreadyExistsException();
    } else if(iFile != null && iParent != null) {
      updateParent = false;
    }

    // if file did not exist, create its INode now
    if(iFile == null) {
      RowKey key = createRowKey(path);
      iFile = new INode(0, false, replication, blockSize, System.currentTimeMillis(), System.currentTimeMillis(),
          masked, clientName, machineName, path.toString().getBytes(), path.toString().getBytes(),
          key, 0, 0);
    } else {
      // if we are overwriting, make sure to get its blocks
      Result nodeInfo = nsTable.get(new Get(iFile.getRowKey().getKey()));
      iFile.getBlocks(nodeInfo).clear();
    }
    // should be generated now, grab it again
    if(iParent == null) {
      iParent = getINode(path.getParent());
    }

    // add file to HBase (update if already exists)
    iFile.putINode(nsTable);
    
    if(updateParent) {
      // get parent result and update parent dirTable.
      Result nodeInfo = nsTable.get(new Get(iParent.getRowKey().getKey()));
      iParent.getDirectoryTable(nodeInfo).addEntry(iFile.getRowKey());
      // commit dirTable to HBase
      iParent.updateDirectory(nsTable);
    }
  }

  /**
   * Method designed to generate a single RowKey. It may try to grab the key
   * from a memory cache.
   * 
   * @param src Used to generate the RowKey
   * @return the RowKey
   * @throws IOException
   */
  private RowKey getRowKey(Path src) throws IOException {
    // try to grab child from cache
    RowKey key = (caching) ? cache.get(src.toString()) : null;

    if(key != null) {
      return key;
    }

    // generate new key (throw exception if not possible)
    key = createRowKey(src);

    return key;
  }

  /**
   * Should only be called in the event that a new RowKey needs to be generated
   * due to create() or mkdirs() file not already existing; this is where the key
   * is cached as well.
   * @param src
   * @return a new RowKey initialized with src
   */
  private RowKey createRowKey(Path src) {
    RowKey key = ReflectionUtils.newInstance(rowKeyClass, null);
    key.setPath(src);

    if(caching)
      cache.put(src.toString(), key);

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

    Path path = new Path(src);
    INode node = getINode(path);
    if(node == null) {
      throw new FileNotFoundException();
    }
    //check parent path first
    Path parent_path = path.getParent();
    if(parent_path == null) {
      throw new FileNotFoundException("Parent does not exist.");
    }
    // then check parent inode
    INode parent = getINode(parent_path);
    if(parent == null)
      throw new FileNotFoundException("Parent does not exist.");
    if(!parent.isDir())
      throw new ParentNotDirectoryException("Parent is not a directory.");

    if(recursive) {
      ArrayList<RowKey> keys = null;
      Result nodeInfo = nsTable.get(new Get(node.getRowKey().getKey()));
      if(node.isDir()) {
        keys = node.getDirectoryTable(nodeInfo).getEntries();
      } else {
        node.getBlocks(nodeInfo);
        node.setState(FileState.DELETED);
        node.putINode(nsTable);
      }
      // delete the child key atomically first
      Delete delete = new Delete(node.getRowKey().getKey());
      nsTable.delete(delete);
      
      // update the parent directory about the deletion
      Result parentNodeInfo = nsTable.get(new Get(parent.getRowKey().getKey()));
      parent.getDirectoryTable(parentNodeInfo)
            .removeEntry(node.getRowKey().getPath().getName());
      parent.updateDirectory(nsTable);

      // delete all of the children, and the children's children...
      // only if the node WAS a directory
      if(keys != null) {
        deleteRecursive(keys);
      }
    } else {
      if(node.isDir()) {
        Result nodeInfo = nsTable.get(new Get(node.getRowKey().getKey()));
        if(!node.getDirectoryTable(nodeInfo).isEmpty())
          return false;
      }

      Delete delete = new Delete(node.getRowKey().getKey());
      nsTable.delete(delete);

      // update parent Node
      Result parentNodeInfo = nsTable.get(new Get(parent.getRowKey().getKey()));
      parent.getDirectoryTable(parentNodeInfo)
            .removeEntry(node.getRowKey().getPath().getName());
      parent.updateDirectory(nsTable);
    }

    // delete time penalty (resolves timestamp milliseconds issue)
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      // do nothing
    }

    return true;
  }

  /** 
   * The recursive function used to delete all children within a directory.
   * General algorithm is to delete children one by one and delete the children of
   * directories if there are any.
   * @param children
   * @throws IOException
   */
  private void deleteRecursive(ArrayList<RowKey> children) throws IOException {
    for(RowKey childKey : children) {
      Result info = nsTable.get(new Get(childKey.getKey()));
      INode node = new INode(childKey, info);

      // if childKey is a directory, recurse thru it
      if(node.isDir()) {
        deleteRecursive(node.getDirectoryTable(info).getEntries());
      } else {
        node.getBlocks(info);
        node.setState(FileState.DELETED);
        node.putINode(nsTable);
      }

      // delete this key after we have deleted all its children
      Delete fileToBeDeleted = new Delete(childKey.getKey());
      nsTable.delete(fileToBeDeleted);
    }
  }

  @Override // ClientProtocol
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    return null;
  }

  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
  }

  /**
   * Must be called before FileSystem can be used!
   * 
   * @param uri
   * 
   * @throws IOException
   */
  public static void format(GiraffaConfiguration conf) throws IOException {
    LOG.info("Format started...");
    String tableName = conf.get(GiraffaConfiguration.GRFA_TABLE_NAME_KEY,
                                GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT);
    URI gURI = FileSystem.getDefaultUri(conf);

    if( ! GiraffaConfiguration.GRFA_URI_SCHEME.equals(gURI.getScheme()))
        throw new IOException("Cannot format. Non-Giraffa URI found: " + gURI);
    HBaseAdmin hbAdmin = new HBaseAdmin(HBaseConfiguration.create(conf));
    if(hbAdmin.tableExists(tableName)) {
      // remove existing table to renew it
      if(hbAdmin.isTableEnabled(tableName)) {
    	  hbAdmin.disableTable(tableName);
      }
      hbAdmin.deleteTable(tableName);
    }

    HTableDescriptor htd = buildGiraffaTable(conf);

    hbAdmin.createTable(htd);
    LOG.info("Created " + tableName);
    hbAdmin.close();

    LOG.info("Format ended... adding work directory.");
  }

  private static HTableDescriptor buildGiraffaTable(GiraffaConfiguration conf)
  throws IOException {
    String tableName = conf.get(GiraffaConfiguration.GRFA_TABLE_NAME_KEY,
        GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT);
    String jarFile = conf.get(GiraffaConfiguration.GRFA_JAR_FILE_KEY,
        GiraffaConfiguration.GRFA_JAR_FILE_DEFAULT);
    String coprocClass = conf.get(GiraffaConfiguration.GRFA_COPROCESSOR_KEY, 
        GiraffaConfiguration.GRFA_COPROCESSOR_DEFAULT);
    Path jarPath = new Path(Util.stringAsURI(jarFile));
    FileSystem jarFs = jarPath.getFileSystem(conf);
    if(!jarFs.exists(jarPath)) {
      LOG.fatal("grfa.jar file is missing!");
      throw new IOException("grfa.jar file is missing in " + jarPath);
    }
    LOG.info("JAR file location is: " + jarPath);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(FileField.getFileAttributes()));
    htd.addCoprocessor(coprocClass, jarPath, Coprocessor.PRIORITY_SYSTEM, null);
    LOG.info("Coprocessor is set to: " + coprocClass);
    return htd;
  }

  @Override // ClientProtocol
  public void fsync(String src, String client) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    throw new IOException("fsync is not supported");
  }

  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    Path path = new Path(src);
    INode iNode = getINode(path);
    if(iNode == null)
      throw new FileNotFoundException();
    
    Result nodeInfo = nsTable.get(new Get(iNode.getRowKey().getKey()));
    ArrayList<LocatedBlock> al = iNode.getBlocks(nodeInfo);
    boolean underConstruction = 
        (iNode.getFileState().equals(FileState.CLOSED)) ? true : false;
    
    LocatedBlocks lbs = new LocatedBlocks(computeFileLength(al),
        underConstruction, al, al.get(al.size()-1), underConstruction);
    return lbs;
  }

  private long computeFileLength(ArrayList<LocatedBlock> al) {
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
    Path path = new Path(src);
    INode node = getINode(path);
    if(node == null)
      throw new FileNotFoundException();
    return node;
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    throw new IOException("symlinks are not supported");
  }

  private INode getINode(Path path) throws IOException {
    RowKey key = getRowKey(path);
    Result nodeInfo = nsTable.get(new Get(key.getKey()));
    if(nodeInfo.isEmpty())
      return null;
    return new INode(key, nodeInfo);
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
    INode node = getINode(new Path(src));

    if(node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if(!node.isDir()) {
      return new DirectoryListing(new HdfsFileStatus[] { getFileInfo(src) }, 0);
    }

    Result nodeInfo = nsTable.get(new Get(node.getRowKey().getKey()));
    ArrayList<RowKey> children = node.getDirectoryTable(nodeInfo).getEntries();
    ArrayList<HdfsFileStatus> list = new ArrayList<HdfsFileStatus>();

    // getListingRecursive(children, list);
    for(RowKey childKey : children)
      list.add(getFileInfo(childKey.getPath().toString()));

    HdfsFileStatus[] retVal = new HdfsFileStatus[list.size()];
    retVal = list.toArray(retVal);
    return new DirectoryListing(retVal, 0);
  }

  @Override // ClientProtocol
  public long getPreferredBlockSize(String fileName) throws IOException,
      UnresolvedLinkException {
    INode inode = getINode(new Path(fileName));
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
    
    Path path = new Path(src);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    String clientName = ugi.getShortUserName();
    String machineName = (ugi.getGroupNames().length == 0) ? "" : ugi.getGroupNames()[0];

    if(path.getParent() == null) {
      //generate root if doesn't exist
      INode root = getINode(path);
      if(root == null) {
        RowKey key = getRowKey(path);
        root = new INode(0, true, (short) 0, 0, System.currentTimeMillis(), System.currentTimeMillis(),
            masked, clientName, machineName, path.toString().getBytes(), path.toString().getBytes(),
            key, 0, 0);
        root.putINode(nsTable);
        return true;
      } else
        throw new IOException("Root has no parent.");
    }

    INode iParent = getINode(path.getParent());
    INode iDir = getINode(path);

    if(!createParent && iParent == null) {
      // SHV !!! make sure parent exists and is a directory
      throw new FileNotFoundException();
    } else if(iParent != null && !iParent.isDir()) {
      throw new ParentNotDirectoryException();
    } else if(createParent && iParent == null) {
      //make the parent directories
      mkdirs(path.getParent().toString(), masked, true);
    } 

    if(iDir != null) {
      return true;
    }
    
    RowKey key = createRowKey(path);
    iDir = new INode(0, true, (short) 0, 0, System.currentTimeMillis(), System.currentTimeMillis(),
        masked, clientName, machineName, path.toString().getBytes(), path.toString().getBytes(),
        key, 0, 0);
    // should be generated now, grab it again
    if(iParent == null) {
      iParent = getINode(path.getParent());
    }

    // add directory to HBase
    iDir.putINode(nsTable);
    
    // grab parent result and update parent dirTable.
    Result nodeInfo = nsTable.get(new Get(iParent.getRowKey().getKey()));
    iParent.getDirectoryTable(nodeInfo).addEntry(iDir.getRowKey());
    // commit dirTable to HBase
    iParent.updateDirectory(nsTable);
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
    
    INode node = getINode(new Path(src));

    if(node == null)
      throw new FileNotFoundException();

    node.updateOwner(nsTable, username, groupname);
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {

    INode node = getINode(new Path(src));

    if(node == null)
      throw new FileNotFoundException();

    node.updatePermissions(nsTable, permission);
  }

  @Override // ClientProtocol
  public void setQuota(String src, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    
    INode node = getINode(new Path(src));

    if(node == null)
      throw new FileNotFoundException();
    //can only set Quota for directories
    if(!node.isDir())
      return;

    node.updateQuotas(nsTable, namespaceQuota, diskspaceQuota);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    INode node = getINode(new Path(src));

    if(node == null)
      return false;
    if(node.isDir())
      return false;

    node.updateReplication(nsTable, replication);
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
    INode node = getINode(new Path(src));

    if(node == null)
      throw new FileNotFoundException();
    if(node.isDir())
      return;

    node.updateTimes(nsTable, mtime, atime);
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

  public void close() throws IOException {
    nsTable.close();
    hbAdmin.close();
  }
}
