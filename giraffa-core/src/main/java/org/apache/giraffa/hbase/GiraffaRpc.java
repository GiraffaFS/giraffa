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

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;

public class GiraffaRpc extends ClientNamenodeProtocolTranslatorPB implements
    NamespaceProtocol {

  private NamespaceProcessorProtos.NamespaceProcessor.BlockingInterface rpcProxy;

  public GiraffaRpc(CoprocessorRpcChannel rpcChannel) {
    super(null);
    rpcProxy =
        NamespaceProcessorProtos.NamespaceProcessor.newBlockingStub(rpcChannel);
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req =
        ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto.newBuilder()
            .setSrc(src).setOffset(offset).setLength(length).build();
    try {
      ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto resp =
          rpcProxy.getBlockLocations(null, req);
      return resp.hasLocations() ? PBHelper.convert(resp.getLocations()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto req =
        ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto.newBuilder()
            .build();
    try {
      return PBHelper.convert(rpcProxy.getServerDefaults(null, req)
          .getServerDefaults());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void create(String src, FsPermission masked, String clientName,
      EnumSetWritable<CreateFlag> flag, boolean createParent,
      short replication, long blockSize) throws AccessControlException,
      AlreadyBeingCreatedException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.CreateRequestProto req =
        ClientNamenodeProtocolProtos.CreateRequestProto.newBuilder()
            .setSrc(src).setMasked(PBHelper.convert(masked))
            .setClientName(clientName)
            .setCreateFlag(PBHelper.convertCreateFlag(flag))
            .setCreateParent(createParent).setReplication(replication)
            .setBlockSize(blockSize).build();
    try {
      rpcProxy.create(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public LocatedBlock append(String src, String clientName)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientNamenodeProtocolProtos.AppendRequestProto req =
        ClientNamenodeProtocolProtos.AppendRequestProto.newBuilder()
            .setSrc(src).setClientName(clientName).build();
    try {
      ClientNamenodeProtocolProtos.AppendResponseProto res =
          rpcProxy.append(null, req);
      return res.hasBlock() ? PBHelper.convert(res.getBlock()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientNamenodeProtocolProtos.SetReplicationRequestProto req =
        ClientNamenodeProtocolProtos.SetReplicationRequestProto.newBuilder()
            .setSrc(src).setReplication(replication).build();
    try {
      return rpcProxy.setReplication(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.SetPermissionRequestProto req =
        ClientNamenodeProtocolProtos.SetPermissionRequestProto.newBuilder()
            .setSrc(src).setPermission(PBHelper.convert(permission)).build();
    try {
      rpcProxy.setPermission(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.SetOwnerRequestProto.Builder req =
        ClientNamenodeProtocolProtos.SetOwnerRequestProto.newBuilder().setSrc(
            src);
    if (username != null)
      req.setUsername(username);
    if (groupname != null)
      req.setGroupname(groupname);
    try {
      rpcProxy.setOwner(null, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.AbandonBlockRequestProto req =
        ClientNamenodeProtocolProtos.AbandonBlockRequestProto.newBuilder()
            .setB(PBHelper.convert(b)).setSrc(src).setHolder(holder).build();
    try {
      rpcProxy.abandonBlock(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludeNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientNamenodeProtocolProtos.AddBlockRequestProto.Builder req =
        ClientNamenodeProtocolProtos.AddBlockRequestProto.newBuilder()
            .setSrc(src).setClientName(clientName);
    if (previous != null)
      req.setPrevious(PBHelper.convert(previous));
    if (excludeNodes != null)
      req.addAllExcludeNodes(Arrays.asList(PBHelper.convert(excludeNodes)));
    try {
      return PBHelper.convert(rpcProxy.addBlock(null, req.build()).getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String src, ExtendedBlock blk,
      DatanodeInfo[] existings, DatanodeInfo[] excludes,
      int numAdditionalNodes, String clientName) throws AccessControlException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto req =
        ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto
            .newBuilder().setSrc(src).setBlk(PBHelper.convert(blk))
            .addAllExistings(Arrays.asList(PBHelper.convert(existings)))
            .addAllExcludes(Arrays.asList(PBHelper.convert(excludes)))
            .setNumAdditionalNodes(numAdditionalNodes)
            .setClientName(clientName).build();
    try {
      return PBHelper.convert(rpcProxy.getAdditionalDatanode(null, req)
          .getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlock last)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.CompleteRequestProto.Builder req =
        ClientNamenodeProtocolProtos.CompleteRequestProto.newBuilder()
            .setSrc(src).setClientName(clientName);
    if (last != null)
      req.setLast(PBHelper.convert(last));
    try {
      return rpcProxy.complete(null, req.build()).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto req =
        ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto.newBuilder()
            .addAllBlocks(Arrays.asList(PBHelper.convertLocatedBlock(blocks)))
            .build();
    try {
      rpcProxy.reportBadBlocks(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean rename(String src, String dst) throws UnresolvedLinkException,
      IOException {
    ClientNamenodeProtocolProtos.RenameRequestProto req =
        ClientNamenodeProtocolProtos.RenameRequestProto.newBuilder()
            .setSrc(src).setDst(dst).build();
    try {
      return rpcProxy.rename(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void rename2(String src, String dst, Options.Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException {
    boolean overwrite = false;
    if (options != null) {
      for (Options.Rename option : options) {
        if (option == Options.Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    ClientNamenodeProtocolProtos.Rename2RequestProto req =
        ClientNamenodeProtocolProtos.Rename2RequestProto.newBuilder()
            .setSrc(src).setDst(dst).setOverwriteDest(overwrite).build();
    try {
      rpcProxy.rename2(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException,
      UnresolvedLinkException {
    ClientNamenodeProtocolProtos.ConcatRequestProto req =
        ClientNamenodeProtocolProtos.ConcatRequestProto.newBuilder()
            .setTrg(trg).addAllSrcs(Arrays.asList(srcs)).build();
    try {
      rpcProxy.concat(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean delete(String src, boolean recursive)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.DeleteRequestProto req =
        ClientNamenodeProtocolProtos.DeleteRequestProto.newBuilder()
            .setSrc(src).setRecursive(recursive).build();
    try {
      return rpcProxy.delete(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientNamenodeProtocolProtos.MkdirsRequestProto req =
        ClientNamenodeProtocolProtos.MkdirsRequestProto.newBuilder()
            .setSrc(src).setMasked(PBHelper.convert(masked))
            .setCreateParent(createParent).build();

    try {
      return rpcProxy.mkdirs(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.GetListingRequestProto req =
        ClientNamenodeProtocolProtos.GetListingRequestProto.newBuilder()
            .setSrc(src).setStartAfter(ByteString.copyFrom(startAfter))
            .setNeedLocation(needLocation).build();
    try {
      ClientNamenodeProtocolProtos.GetListingResponseProto result =
          rpcProxy.getListing(null, req);

      if (result.hasDirList()) {
        return PBHelper.convert(result.getDirList());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void renewLease(String clientName) throws AccessControlException,
      IOException {
    ClientNamenodeProtocolProtos.RenewLeaseRequestProto req =
        ClientNamenodeProtocolProtos.RenewLeaseRequestProto.newBuilder()
            .setClientName(clientName).build();
    try {
      rpcProxy.renewLease(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    ClientNamenodeProtocolProtos.RecoverLeaseRequestProto req =
        ClientNamenodeProtocolProtos.RecoverLeaseRequestProto.newBuilder()
            .setSrc(src).setClientName(clientName).build();
    try {
      return rpcProxy.recoverLease(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long[] getStats() throws IOException {
    ClientNamenodeProtocolProtos.GetFsStatusRequestProto req =
        ClientNamenodeProtocolProtos.GetFsStatusRequestProto.newBuilder()
            .build();
    try {
      return PBHelper.convert(rpcProxy.getFsStats(null, req));
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public DatanodeInfo[]
      getDatanodeReport(HdfsConstants.DatanodeReportType type)
          throws IOException {
    ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto req =
        ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto.newBuilder()
            .setType(PBHelper.convert(type)).build();
    try {
      return PBHelper
          .convert(rpcProxy.getDatanodeReport(null, req).getDiList());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException,
      UnresolvedLinkException {
    ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto req =
        ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto
            .newBuilder().setFilename(filename).build();
    try {
      return rpcProxy.getPreferredBlockSize(null, req).getBsize();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action)
      throws IOException {
    ClientNamenodeProtocolProtos.SetSafeModeRequestProto req =
        ClientNamenodeProtocolProtos.SetSafeModeRequestProto.newBuilder()
            .setAction(PBHelper.convert(action)).build();
    try {
      return rpcProxy.setSafeMode(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void saveNamespace() throws AccessControlException, IOException {
    ClientNamenodeProtocolProtos.SaveNamespaceRequestProto req =
        ClientNamenodeProtocolProtos.SaveNamespaceRequestProto.newBuilder()
            .build();
    try {
      rpcProxy.saveNamespace(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long rollEdits() throws AccessControlException, IOException {
    ClientNamenodeProtocolProtos.RollEditsRequestProto req =
        ClientNamenodeProtocolProtos.RollEditsRequestProto.getDefaultInstance();
    try {
      ClientNamenodeProtocolProtos.RollEditsResponseProto resp =
          rpcProxy.rollEdits(null, req);
      return resp.getNewSegmentTxId();
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public boolean restoreFailedStorage(String arg)
      throws AccessControlException, IOException {
    ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto req =
        ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto
            .newBuilder().setArg(arg).build();
    try {
      return rpcProxy.restoreFailedStorage(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void refreshNodes() throws IOException {
    ClientNamenodeProtocolProtos.RefreshNodesRequestProto req =
        ClientNamenodeProtocolProtos.RefreshNodesRequestProto.newBuilder()
            .build();
    try {
      rpcProxy.refreshNodes(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto req =
        ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto.newBuilder()
            .build();
    try {
      rpcProxy.finalizeUpgrade(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto.Builder req =
        ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto
            .newBuilder().setPath(path);
    if (cookie != null)
      req.setCookie(cookie);
    try {
      return PBHelper.convert(rpcProxy.listCorruptFileBlocks(null, req.build())
          .getCorrupt());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void metaSave(String filename) throws IOException {
    ClientNamenodeProtocolProtos.MetaSaveRequestProto req =
        ClientNamenodeProtocolProtos.MetaSaveRequestProto.newBuilder()
            .setFilename(filename).build();
    try {
      rpcProxy.metaSave(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.GetFileInfoRequestProto req =
        ClientNamenodeProtocolProtos.GetFileInfoRequestProto.newBuilder()
            .setSrc(src).build();
    try {
      ClientNamenodeProtocolProtos.GetFileInfoResponseProto res =
          rpcProxy.getFileInfo(null, req);
      return res.hasFs() ? PBHelper.convert(res.getFs()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto req =
        ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto.newBuilder()
            .setSrc(src).build();
    try {
      ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto result =
          rpcProxy.getFileLinkInfo(null, req);
      return result.hasFs() ? PBHelper.convert(rpcProxy.getFileLinkInfo(null,
          req).getFs()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public ContentSummary getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.GetContentSummaryRequestProto req =
        ClientNamenodeProtocolProtos.GetContentSummaryRequestProto.newBuilder()
            .setPath(path).build();
    try {
      return PBHelper.convert(rpcProxy.getContentSummary(null, req)
          .getSummary());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.SetQuotaRequestProto req =
        ClientNamenodeProtocolProtos.SetQuotaRequestProto.newBuilder()
            .setPath(path).setNamespaceQuota(namespaceQuota)
            .setDiskspaceQuota(diskspaceQuota).build();
    try {
      rpcProxy.setQuota(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void fsync(String src, String client) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.FsyncRequestProto req =
        ClientNamenodeProtocolProtos.FsyncRequestProto.newBuilder().setSrc(src)
            .setClient(client).build();
    try {
      rpcProxy.fsync(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientNamenodeProtocolProtos.SetTimesRequestProto req =
        ClientNamenodeProtocolProtos.SetTimesRequestProto.newBuilder()
            .setSrc(src).setMtime(mtime).setAtime(atime).build();
    try {
      rpcProxy.setTimes(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void createSymlink(String target, String link, FsPermission dirPerm,
      boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientNamenodeProtocolProtos.CreateSymlinkRequestProto req =
        ClientNamenodeProtocolProtos.CreateSymlinkRequestProto.newBuilder()
            .setTarget(target).setLink(link)
            .setDirPerm(PBHelper.convert(dirPerm))
            .setCreateParent(createParent).build();
    try {
      rpcProxy.createSymlink(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public String getLinkTarget(String path) throws AccessControlException,
      FileNotFoundException, IOException {
    ClientNamenodeProtocolProtos.GetLinkTargetRequestProto req =
        ClientNamenodeProtocolProtos.GetLinkTargetRequestProto.newBuilder()
            .setPath(path).build();
    try {
      return rpcProxy.getLinkTarget(null, req).getTargetPath();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException {
    ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto req =
        ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto
            .newBuilder().setBlock(PBHelper.convert(block))
            .setClientName(clientName).build();
    try {
      return PBHelper.convert(rpcProxy.updateBlockForPipeline(null, req)
          .getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes) throws IOException {
    ClientNamenodeProtocolProtos.UpdatePipelineRequestProto req =
        ClientNamenodeProtocolProtos.UpdatePipelineRequestProto.newBuilder()
            .setClientName(clientName).setOldBlock(PBHelper.convert(oldBlock))
            .setNewBlock(PBHelper.convert(newBlock))
            .addAllNewNodes(Arrays.asList(PBHelper.convert(newNodes))).build();
    try {
      rpcProxy.updatePipeline(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    ClientNamenodeProtocolProtos.GetDelegationTokenRequestProto req =
        ClientNamenodeProtocolProtos.GetDelegationTokenRequestProto
            .newBuilder().setRenewer(renewer.toString()).build();
    try {
      return PBHelper.convertDelegationToken(rpcProxy.getDelegationToken(null,
          req).getToken());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    ClientNamenodeProtocolProtos.RenewDelegationTokenRequestProto req =
        ClientNamenodeProtocolProtos.RenewDelegationTokenRequestProto
            .newBuilder().setToken(PBHelper.convert(token)).build();
    try {
      return rpcProxy.renewDelegationToken(null, req).getNewExireTime();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    ClientNamenodeProtocolProtos.CancelDelegationTokenRequestProto req =
        ClientNamenodeProtocolProtos.CancelDelegationTokenRequestProto
            .newBuilder().setToken(PBHelper.convert(token)).build();
    try {
      rpcProxy.cancelDelegationToken(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto req =
        ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto
            .newBuilder().setBandwidth(bandwidth).build();
    try {
      rpcProxy.setBalancerBandwidth(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        ClientNamenodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(ClientNamenodeProtocolPB.class), methodName);
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto req =
        ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto
            .newBuilder().build();
    try {
      return PBHelper.convert(rpcProxy.getDataEncryptionKey(null, req)
          .getDataEncryptionKey());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
}
