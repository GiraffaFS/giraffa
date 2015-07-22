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

import static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName;
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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FileField;
import org.apache.giraffa.GiraffaConfiguration;
import org.apache.giraffa.NamespaceService;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.RowKeyFactory;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
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
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseResponseProto;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.ServiceException;

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

  private Admin hbAdmin;
  private Table nsTable;
  private Connection connection;
  private FsServerDefaults serverDefaults;

  private static final Log LOG =
    LogFactory.getLog(NamespaceAgent.class.getName());

  public NamespaceAgent() {}

  @Override // NamespaceService
  public void initialize(GiraffaConfiguration conf) throws IOException {
    RowKeyFactory.registerRowKey(conf);
    this.connection = ConnectionFactory.createConnection(conf);
    this.hbAdmin = connection.getAdmin();
    String tableName = getGiraffaTableName(conf);

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

    try {
      this.nsTable = connection.getTable(TableName.valueOf(tableName));
    } catch(TableNotFoundException tnfe) {
      throw new IOException("Giraffa is not formatted.", tnfe);
    }
  }

  private ClientProtocol getRegionProxy(String src) throws IOException {
    return getRegionProxy(RowKeyFactory.newInstance(src));
  }

  private ClientProtocol getRegionProxy(RowKey key) {
    // load blocking stub for protocol based on row key
    CoprocessorRpcChannel channel = nsTable.coprocessorService(key.getKey());
    final ClientNamenodeProtocol.BlockingInterface stub =
        ClientNamenodeProtocol.newBlockingStub(channel);
    
    // create a handler for forwarding proxy calls to blocking stub
    InvocationHandler handler = new InvocationHandler(){
      @Override
      public Object invoke(Object proxy, Method method, Object[] args)
          throws Throwable {
        try {
          return method.invoke(stub, args);
        }catch(InvocationTargetException e) {
          if(LOG.isDebugEnabled()) {
            LOG.debug(ProtobufHelper.getRemoteException(
                (ServiceException) e.getCause()).getMessage());
          }
          throw e.getCause();
        }
      }
    };
    
    // create a proxy implementation of ClientNamenodeProtocolPB
    Class<ClientNamenodeProtocolPB> classObj = ClientNamenodeProtocolPB.class;
    ClientNamenodeProtocolPB clientPB =
        (ClientNamenodeProtocolPB) Proxy.newProxyInstance(
            classObj.getClassLoader(), new Class[]{classObj}, handler);
    
    return new ClientNamenodeProtocolTranslatorPB(clientPB);
  }

  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
                           String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(src);
    proxy.abandonBlock(b, fileId, src, holder);
  }

  @Override // ClientProtocol
  public LocatedBlock addBlock(String src, String clientName,
                               ExtendedBlock previous,
                               DatanodeInfo[] excludeNodes, long fileId,
                               String[] favoredNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientProtocol proxy = getRegionProxy(src);
    LocatedBlock blk = proxy.addBlock(src, clientName, previous, excludeNodes,
        fileId, favoredNodes);
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
  public boolean complete(String src, String clientName, ExtendedBlock last,
                          long fileId)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(src);
    boolean res = proxy.complete(src, clientName, last, fileId);
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
  public HdfsFileStatus create(
      String src, FsPermission masked, String clientName,
      EnumSetWritable<CreateFlag> createFlag, boolean createParent,
      short replication, long blockSize) throws AccessControlException,
      AlreadyBeingCreatedException, DSQuotaExceededException,
      NSQuotaExceededException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    if(new Path(src).getParent() == null)
      throw new IOException("Root cannot be a file.");

    ClientProtocol proxy = getRegionProxy(src);
    return proxy.create(src, masked, clientName, createFlag, createParent,
        replication, blockSize);
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

    ClientProtocol proxy = getRegionProxy(src);
    return proxy.delete(src, recursive);
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

   /**
   * Must be called before FileSystem can be used!
   * 
   * @param conf
   * 
   * @throws IOException
   */
  @Override // NamespaceService
  public void format(GiraffaConfiguration conf) throws IOException {
    LOG.info("Format started...");
    String tblString = getGiraffaTableName(conf);
    TableName tableName = TableName.valueOf(tblString);
    URI gURI = FileSystem.getDefaultUri(conf);

    if( ! GiraffaConfiguration.GRFA_URI_SCHEME.equals(gURI.getScheme()))
        throw new IOException("Cannot format. Non-Giraffa URI found: " + gURI);
    connection = ConnectionFactory.createConnection(conf);
    hbAdmin = connection.getAdmin();
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
    LOG.info("Format ended... adding work directory.");
  }

  private static HTableDescriptor buildGiraffaTable(GiraffaConfiguration conf)
  throws IOException {
    String tableName = getGiraffaTableName(conf);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.addFamily(new HColumnDescriptor(FileField.getFileAttributes()));
    htd.addFamily(new HColumnDescriptor(FileField.getFileExtendedAttributes()));
    String coprocClass =
        conf.get(GRFA_COPROCESSOR_KEY, GRFA_COPROCESSOR_DEFAULT);
    htd.addCoprocessor(coprocClass, null, Coprocessor.PRIORITY_SYSTEM, null);
    LOG.info("Block management processor is set to: " + coprocClass);
    String nsProcClass = conf.get(
        GRFA_NAMESPACE_PROCESSOR_KEY, GRFA_NAMESPACE_PROCESSOR_DEFAULT);
    htd.addCoprocessor(nsProcClass, null, Coprocessor.PRIORITY_SYSTEM, null);
    LOG.info("Namespace processor is set to: " + nsProcClass);
    return htd;
  }

  @Override // ClientProtocol
  public void fsync(String src, long inodeId, String client,
                    long lastBlockLength)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    // NYI, but can't throw Exception for sake of hflush()
  }

  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(src);
    LocatedBlocks lbs = proxy.getBlockLocations(src, offset, length);
    if(lbs == null)
      throw new FileNotFoundException("File does not exist: " + src);
    return lbs;
  }

  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(path);
    ContentSummary summary = proxy.getContentSummary(path);
    return summary;
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
    ClientProtocol proxy = getRegionProxy(src);
    HdfsFileStatus fStatus = proxy.getFileInfo(src);
    return fStatus;
  }

   @Override
   public boolean isFileClosed(String src)
       throws AccessControlException, FileNotFoundException,
              UnresolvedLinkException, IOException {
     return getRegionProxy(src).isFileClosed(src);
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
      throws IOException {
    if(startAfter == null)
      startAfter = new byte[0];
    ClientProtocol proxy = getRegionProxy(src);
    DirectoryListing files = proxy.getListing(src, startAfter, needLocation);
    return files;
  }

   @Override
   public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
       throws IOException {
     return new SnapshottableDirectoryStatus[0];
   }

   @Override // ClientProtocol
  public long getPreferredBlockSize(String filename) throws IOException,
      UnresolvedLinkException {
    ClientProtocol proxy = getRegionProxy(filename);
    long blockSize = proxy.getPreferredBlockSize(filename);
    if(blockSize < 0)
      throw new FileNotFoundException("File does not exist: " + filename);
    return blockSize;
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
    ClientProtocol proxy = getRegionProxy(src);
    boolean created = proxy.mkdirs(src, masked, createParent);
    if(!createParent && !created)
      throw new FileNotFoundException("File does not exist: " + src);
    return created;
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName) throws IOException {
    ClientProtocol proxy = getRegionProxy(src);
    return proxy.recoverLease(src, clientName);
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    throw new IOException("refreshNodes is not supported");
  }

  @Override // ClientProtocol
  public boolean rename(String src, String dst) throws UnresolvedLinkException,
      IOException {
    ClientProtocol proxy = getRegionProxy(src);
    return proxy.rename(src, dst);
  }

  @Override // ClientProtocol
  public void rename2(String src, String dst, Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(src);
    proxy.rename2(src, dst, options);
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    return 0;
  }

  @Override // ClientProtocol
  public void renewLease(final String clientName)
      throws AccessControlException, IOException {
    try {
      nsTable.coprocessorService(ClientNamenodeProtocol.class,
          HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_START_ROW,
              new Batch.Call<ClientNamenodeProtocol, Void>() {
                  @Override
                  public Void call(ClientNamenodeProtocol instance)
                      throws IOException {
                    RenewLeaseRequestProto req =
                        RenewLeaseRequestProto.newBuilder()
                            .setClientName(clientName).build();
                    ServerRpcController controller = new ServerRpcController();
                    RpcCallback<RenewLeaseResponseProto> rpcCallback =
                        new BlockingRpcCallback<RenewLeaseResponseProto>();
                    instance.renewLease(controller, req, rpcCallback);
                    return null;
                  }
              });
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
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
    ClientProtocol proxy = getRegionProxy(src);
    proxy.setOwner(src, username, groupname);
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(src);
    proxy.setPermission(src, permission);
  }

  @Override // ClientProtocol
  public void setQuota(String src, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(src);
    proxy.setQuota(src, namespaceQuota, diskspaceQuota);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientProtocol proxy = getRegionProxy(src);
    return proxy.setReplication(src, replication);
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    return false;
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(src);
    proxy.setTimes(src, mtime, atime);
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
      throws IOException {
    return null;
  }

  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
                             ExtendedBlock newBlock, DatanodeID[] newNodes,
                             String[] newStorageIds)
      throws IOException {
  }

  @Override // NamespaceService
  public void close() throws IOException {
    if(nsTable != null) nsTable.close();
    nsTable = null;
    if(hbAdmin != null) hbAdmin.close();
    hbAdmin = null;
    if(connection != null) connection.close();
    connection = null;
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String src, long fileId,
                                            ExtendedBlock blk,
                                            DatanodeInfo[] existings,
                                            String[] existingStorageIds,
                                            DatanodeInfo[] excludes,
                                            int numAdditionalNodes,
      String clientName) throws AccessControlException, FileNotFoundException,
      SafeModeException, UnresolvedLinkException, IOException {
    ClientProtocol proxy = getRegionProxy(src);
    return proxy.getAdditionalDatanode(src, fileId, blk, existings,
        existingStorageIds, excludes, numAdditionalNodes, clientName);
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
  @Idempotent
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
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
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
  public BatchedEntries<CachePoolEntry> listCachePools(String prevPool)
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
    ClientProtocol proxy = getRegionProxy(src);
    proxy.setXAttr(src, xAttr, flag);
  }

  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    ClientProtocol proxy = getRegionProxy(src);
    return proxy.getXAttrs(src, xAttrs);
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    ClientProtocol proxy = getRegionProxy(src);
    return proxy.listXAttrs(src);
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    ClientProtocol proxy = getRegionProxy(src);
    proxy.removeXAttr(src, xAttr);
  }
}
