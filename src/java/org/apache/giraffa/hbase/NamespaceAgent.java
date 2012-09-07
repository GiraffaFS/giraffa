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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FileField;
import org.apache.giraffa.GiraffaConfiguration;
import org.apache.giraffa.NamespaceService;
import org.apache.giraffa.RowKey;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
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
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

 /**
  * NamespaceAgent is the proxy used by DFSClient to communicate with HBase
  * as if it is a NameNode.
  * NamespaceAgent implements ClientProtocol and is a replacement of the 
  * NameNode RPC proxy.
  */
public class NamespaceAgent implements NamespaceService {
  public static final String  GRFA_COPROCESSOR_KEY = "grfa.coprocessor.class";
  public static final String  GRFA_COPROCESSOR_DEFAULT =
                                  BlockManagementAgent.class.getName();
  public static final String  GRFA_NAMESPACE_PROCESSOR_KEY = 
                                  "grfa.namespace.processor.class"; 
  public static final String  GRFA_NAMESPACE_PROCESSOR_DEFAULT =
                                  NamespaceProcessor.class.getName();

  public static enum BlockAction {
    CLOSE, ALLOCATE, DELETE
  }

  private Class<? extends RowKey> rowKeyClass;
  private boolean caching;

  private HBaseAdmin hbAdmin;
  private HTableInterface nsTable;

  private HashMap<String, RowKey> cache = new HashMap<String, RowKey>();

  private static final Log LOG =
    LogFactory.getLog(NamespaceAgent.class.getName());

  public NamespaceAgent() {}

  @Override // NamespaceService
  public void initialize(GiraffaConfiguration conf) throws IOException {
    rowKeyClass = conf.getClass(GiraffaConfiguration.GRFA_ROW_KEY_KEY,
                                GiraffaConfiguration.GRFA_ROW_KEY_DEFAULT,
                                RowKey.class);
    caching = conf.getBoolean(GiraffaConfiguration.GRFA_CACHING_KEY,
                              GiraffaConfiguration.GRFA_CACHING_DEFAULT);
    LOG.info("Caching is set to: " + caching);
    LOG.info("RowKey is set to: " + rowKeyClass.getCanonicalName());
    this.hbAdmin = new HBaseAdmin(HBaseConfiguration.create(conf));
    String tableName = conf.get(GiraffaConfiguration.GRFA_TABLE_NAME_KEY,
        GiraffaConfiguration.GRFA_TABLE_NAME_DEFAULT);

    try {
      this.nsTable = new HTable(hbAdmin.getConfiguration(), tableName);
    } catch(TableNotFoundException tnfe) {
      throw new IOException("Giraffa is not formatted.", tnfe);
    }
  }

  private NamespaceProtocol getRegionProxy(String src) throws IOException {
    return getRegionProxy(getRowKey(new Path(src)));
  }

  NamespaceProtocol getRegionProxy(RowKey key) throws IOException {
    return nsTable.coprocessorProxy(NamespaceProtocol.class, key.getKey());
  }

  @Override // ClientProtocol
  public void abandonBlock(Block b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    NamespaceProtocol proxy = getRegionProxy(src);
    proxy.abandonBlock(b, src, holder);
  }

  @Override // ClientProtocol
  public LocatedBlock addBlock(
      String src, String clientName, Block previous, DatanodeInfo[] excludeNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    NamespaceProtocol proxy = getRegionProxy(src);
    LocatedBlock blk = proxy.addBlock(src, clientName, previous, excludeNodes);
    if(blk == null)
      throw new FileNotFoundException("File does not exist: " + src);
    LOG.info("Added block " + blk + " to file: " + src);
    return blk;
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
    NamespaceProtocol proxy = getRegionProxy(src);
    boolean res = proxy.complete(src, clientName, last);
    if(!res)
      throw new FileNotFoundException("File does not exist: " + src);
    LOG.info("File: " + src + " is " + (res ? "completed" : "not completed"));
    return res;
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
    if(new Path(src).getParent() == null)
      throw new IOException("Root cannot be a file.");

    NamespaceProtocol proxy = getRegionProxy(src);
    proxy.create(src, masked, clientName, createFlag, createParent,
        replication, blockSize);
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
    //check parent path first
    Path parentPath = path.getParent();
    if(parentPath == null) {
      throw new FileNotFoundException("Parent does not exist.");
    }

    NamespaceProtocol proxy = getRegionProxy(src);
    return proxy.delete(src, recursive);
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

  /**
   * Must be called before FileSystem can be used!
   * 
   * @param uri
   * 
   * @throws IOException
   */
  @Override // NamespaceService
  public void format(GiraffaConfiguration conf) throws IOException {
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
    String coprocClass =
        conf.get(GRFA_COPROCESSOR_KEY, GRFA_COPROCESSOR_DEFAULT);
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
    LOG.info("Block management processor is set to: " + coprocClass);

    String nsProcClass = conf.get(
        GRFA_NAMESPACE_PROCESSOR_KEY, GRFA_NAMESPACE_PROCESSOR_DEFAULT);
    htd.addCoprocessor(nsProcClass, jarPath, Coprocessor.PRIORITY_SYSTEM, null);
    LOG.info("Namespace processor is set to: " + nsProcClass);
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
    NamespaceProtocol proxy = getRegionProxy(src);
    LocatedBlocks lbs = proxy.getBlockLocations(src, offset, length);
    if(lbs == null)
      throw new FileNotFoundException("File does not exist: " + src);
    return lbs;
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
    NamespaceProtocol proxy = getRegionProxy(src);
    HdfsFileStatus fStatus = proxy.getFileInfo(src);
    if(fStatus == null)
      throw new FileNotFoundException("File does not exist: " + src);
    return fStatus;
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
    NamespaceProtocol proxy = getRegionProxy(src);
    DirectoryListing files = proxy.getListing(src, startAfter, needLocation);
    if(files == null)
      throw new FileNotFoundException("File does not exist: " + src);
    return files;
  }

  @Override // ClientProtocol
  public long getPreferredBlockSize(String filename) throws IOException,
      UnresolvedLinkException {
    NamespaceProtocol proxy = getRegionProxy(filename);
    long blockSize = proxy.getPreferredBlockSize(filename);
    if(blockSize < 0)
      throw new FileNotFoundException("File does not exist: " + filename);
    return blockSize;
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
    NamespaceProtocol proxy = getRegionProxy(src);
    boolean created = proxy.mkdirs(src, masked, createParent);
    if(!createParent && !created)
      throw new FileNotFoundException("File does not exist: " + src);
    return created;
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
    NamespaceProtocol proxy = getRegionProxy(src);
    proxy.setOwner(src, username, groupname);
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    NamespaceProtocol proxy = getRegionProxy(src);
    proxy.setPermission(src, permission);
  }

  @Override // ClientProtocol
  public void setQuota(String src, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    NamespaceProtocol proxy = getRegionProxy(src);
    proxy.setQuota(src, namespaceQuota, diskspaceQuota);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    NamespaceProtocol proxy = getRegionProxy(src);
    return proxy.setReplication(src, replication);
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return false;
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    NamespaceProtocol proxy = getRegionProxy(src);
    proxy.setTimes(src, mtime, atime);
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

  @Override // NamespaceService
  public void close() throws IOException {
    nsTable.close();
    hbAdmin.close();
  }
}
