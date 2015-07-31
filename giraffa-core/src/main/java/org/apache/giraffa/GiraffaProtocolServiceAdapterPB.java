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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.giraffa.GiraffaProtos.GiraffaProtocolService.BlockingInterface;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AllowSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AllowSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2RequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2ResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetOwnerRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetOwnerResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetPermissionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetPermissionResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetQuotaRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetQuotaResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetTimesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetTimesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdatePipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdatePipelineResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrResponseProto;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;

public class GiraffaProtocolServiceAdapterPB
    implements ClientNamenodeProtocolPB {

  private final BlockingInterface service;

  public GiraffaProtocolServiceAdapterPB(BlockingInterface service) {
    this.service = service;
  }

  @Override
  public GetBlockLocationsResponseProto getBlockLocations(
      RpcController controller,
      GetBlockLocationsRequestProto request)
      throws ServiceException {
    return service.getBlockLocations(controller, request);
  }

  @Override
  public GetServerDefaultsResponseProto getServerDefaults(
      RpcController controller,
      GetServerDefaultsRequestProto request)
      throws ServiceException {
    return service.getServerDefaults(controller, request);
  }

  @Override
  public CreateResponseProto create(
      RpcController controller,
      CreateRequestProto request)
      throws ServiceException {
    return service.create(controller, request);
  }

  @Override
  public AppendResponseProto append(
      RpcController controller,
      AppendRequestProto request)
      throws ServiceException {
    return service.append(controller, request);
  }

  @Override
  public SetReplicationResponseProto setReplication(
      RpcController controller,
      SetReplicationRequestProto request)
      throws ServiceException {
    return service.setReplication(controller, request);
  }

  @Override
  public SetPermissionResponseProto setPermission(
      RpcController controller,
      SetPermissionRequestProto request)
      throws ServiceException {
    return service.setPermission(controller, request);
  }

  @Override
  public SetOwnerResponseProto setOwner(
      RpcController controller,
      SetOwnerRequestProto request)
      throws ServiceException {
    return service.setOwner(controller, request);
  }

  @Override
  public AbandonBlockResponseProto abandonBlock(
      RpcController controller,
      AbandonBlockRequestProto request)
      throws ServiceException {
    return service.abandonBlock(controller, request);
  }

  @Override
  public AddBlockResponseProto addBlock(
      RpcController controller,
      AddBlockRequestProto request)
      throws ServiceException {
    return service.addBlock(controller, request);
  }

  @Override
  public GetAdditionalDatanodeResponseProto getAdditionalDatanode(
      RpcController controller,
      GetAdditionalDatanodeRequestProto request)
      throws ServiceException {
    return service.getAdditionalDatanode(controller, request);
  }

  @Override
  public CompleteResponseProto complete(
      RpcController controller,
      CompleteRequestProto request)
      throws ServiceException {
    return service.complete(controller, request);
  }

  @Override
  public ReportBadBlocksResponseProto reportBadBlocks(
      RpcController controller,
      ReportBadBlocksRequestProto request)
      throws ServiceException {
    return service.reportBadBlocks(controller, request);
  }

  @Override
  public ConcatResponseProto concat(
      RpcController controller,
      ConcatRequestProto request)
      throws ServiceException {
    return service.concat(controller, request);
  }

  @Override
  public RenameResponseProto rename(
      RpcController controller,
      RenameRequestProto request)
      throws ServiceException {
    return service.rename(controller, request);
  }

  @Override
  public Rename2ResponseProto rename2(
      RpcController controller,
      Rename2RequestProto request)
      throws ServiceException {
    return service.rename2(controller, request);
  }

  @Override
  public DeleteResponseProto delete(
      RpcController controller,
      DeleteRequestProto request)
      throws ServiceException {
    return service.delete(controller, request);
  }

  @Override
  public MkdirsResponseProto mkdirs(
      RpcController controller,
      MkdirsRequestProto request)
      throws ServiceException {
    return service.mkdirs(controller, request);
  }

  @Override
  public GetListingResponseProto getListing(
      RpcController controller,
      GetListingRequestProto request)
      throws ServiceException {
    return service.getListing(controller, request);
  }

  @Override
  public RenewLeaseResponseProto renewLease(
      RpcController controller,
      RenewLeaseRequestProto request)
      throws ServiceException {
    return service.renewLease(controller, request);
  }

  @Override
  public RecoverLeaseResponseProto recoverLease(
      RpcController controller,
      RecoverLeaseRequestProto request)
      throws ServiceException {
    return service.recoverLease(controller, request);
  }

  @Override
  public GetFsStatsResponseProto getFsStats(
      RpcController controller,
      GetFsStatusRequestProto request)
      throws ServiceException {
    return service.getFsStats(controller, request);
  }

  @Override
  public GetDatanodeReportResponseProto getDatanodeReport(
      RpcController controller,
      GetDatanodeReportRequestProto request)
      throws ServiceException {
    return service.getDatanodeReport(controller, request);
  }

  @Override
  public GetPreferredBlockSizeResponseProto getPreferredBlockSize(
      RpcController controller,
      GetPreferredBlockSizeRequestProto request)
      throws ServiceException {
    return service.getPreferredBlockSize(controller, request);
  }

  @Override
  public SetSafeModeResponseProto setSafeMode(
      RpcController controller,
      SetSafeModeRequestProto request)
      throws ServiceException {
    return service.setSafeMode(controller, request);
  }

  @Override
  public SaveNamespaceResponseProto saveNamespace(
      RpcController controller,
      SaveNamespaceRequestProto request)
      throws ServiceException {
    return service.saveNamespace(controller, request);
  }

  @Override
  public RollEditsResponseProto rollEdits(
      RpcController controller,
      RollEditsRequestProto request)
      throws ServiceException {
    return service.rollEdits(controller, request);
  }

  @Override
  public RestoreFailedStorageResponseProto restoreFailedStorage(
      RpcController controller,
      RestoreFailedStorageRequestProto request)
      throws ServiceException {
    return service.restoreFailedStorage(controller, request);
  }

  @Override
  public RefreshNodesResponseProto refreshNodes(
      RpcController controller,
      RefreshNodesRequestProto request)
      throws ServiceException {
    return service.refreshNodes(controller, request);
  }

  @Override
  public FinalizeUpgradeResponseProto finalizeUpgrade(
      RpcController controller,
      FinalizeUpgradeRequestProto request)
      throws ServiceException {
    return service.finalizeUpgrade(controller, request);
  }

  @Override
  public RollingUpgradeResponseProto rollingUpgrade(
      RpcController controller,
      RollingUpgradeRequestProto request)
      throws ServiceException {
    return service.rollingUpgrade(controller, request);
  }

  @Override
  public ListCorruptFileBlocksResponseProto listCorruptFileBlocks(
      RpcController controller,
      ListCorruptFileBlocksRequestProto request)
      throws ServiceException {
    return service.listCorruptFileBlocks(controller, request);
  }

  @Override
  public MetaSaveResponseProto metaSave(
      RpcController controller,
      MetaSaveRequestProto request)
      throws ServiceException {
    return service.metaSave(controller, request);
  }

  @Override
  public GetFileInfoResponseProto getFileInfo(
      RpcController controller,
      GetFileInfoRequestProto request)
      throws ServiceException {
    return service.getFileInfo(controller, request);
  }

  @Override
  public AddCacheDirectiveResponseProto addCacheDirective(
      RpcController controller,
      AddCacheDirectiveRequestProto request)
      throws ServiceException {
    return service.addCacheDirective(controller, request);
  }

  @Override
  public ModifyCacheDirectiveResponseProto modifyCacheDirective(
      RpcController controller,
      ModifyCacheDirectiveRequestProto request)
      throws ServiceException {
    return service.modifyCacheDirective(controller, request);
  }

  @Override
  public RemoveCacheDirectiveResponseProto removeCacheDirective(
      RpcController controller,
      RemoveCacheDirectiveRequestProto request)
      throws ServiceException {
    return service.removeCacheDirective(controller, request);
  }

  @Override
  public ListCacheDirectivesResponseProto listCacheDirectives(
      RpcController controller,
      ListCacheDirectivesRequestProto request)
      throws ServiceException {
    return service.listCacheDirectives(controller, request);
  }

  @Override
  public AddCachePoolResponseProto addCachePool(
      RpcController controller,
      AddCachePoolRequestProto request)
      throws ServiceException {
    return service.addCachePool(controller, request);
  }

  @Override
  public ModifyCachePoolResponseProto modifyCachePool(
      RpcController controller,
      ModifyCachePoolRequestProto request)
      throws ServiceException {
    return service.modifyCachePool(controller, request);
  }

  @Override
  public RemoveCachePoolResponseProto removeCachePool(
      RpcController controller,
      RemoveCachePoolRequestProto request)
      throws ServiceException {
    return service.removeCachePool(controller, request);
  }

  @Override
  public ListCachePoolsResponseProto listCachePools(
      RpcController controller,
      ListCachePoolsRequestProto request)
      throws ServiceException {
    return service.listCachePools(controller, request);
  }

  @Override
  public GetFileLinkInfoResponseProto getFileLinkInfo(
      RpcController controller,
      GetFileLinkInfoRequestProto request)
      throws ServiceException {
    return service.getFileLinkInfo(controller, request);
  }

  @Override
  public GetContentSummaryResponseProto getContentSummary(
      RpcController controller,
      GetContentSummaryRequestProto request)
      throws ServiceException {
    return service.getContentSummary(controller, request);
  }

  @Override
  public SetQuotaResponseProto setQuota(
      RpcController controller,
      SetQuotaRequestProto request)
      throws ServiceException {
    return service.setQuota(controller, request);
  }

  @Override
  public FsyncResponseProto fsync(
      RpcController controller,
      FsyncRequestProto request)
      throws ServiceException {
    return service.fsync(controller, request);
  }

  @Override
  public SetTimesResponseProto setTimes(
      RpcController controller,
      SetTimesRequestProto request)
      throws ServiceException {
    return service.setTimes(controller, request);
  }

  @Override
  public CreateSymlinkResponseProto createSymlink(
      RpcController controller,
      CreateSymlinkRequestProto request)
      throws ServiceException {
    return service.createSymlink(controller, request);
  }

  @Override
  public GetLinkTargetResponseProto getLinkTarget(
      RpcController controller,
      GetLinkTargetRequestProto request)
      throws ServiceException {
    return service.getLinkTarget(controller, request);
  }

  @Override
  public UpdateBlockForPipelineResponseProto updateBlockForPipeline(
      RpcController controller,
      UpdateBlockForPipelineRequestProto request)
      throws ServiceException {
    return service.updateBlockForPipeline(controller, request);
  }

  @Override
  public UpdatePipelineResponseProto updatePipeline(
      RpcController controller,
      UpdatePipelineRequestProto request)
      throws ServiceException {
    return service.updatePipeline(controller, request);
  }

  @Override
  public GetDelegationTokenResponseProto getDelegationToken(
      RpcController controller,
      GetDelegationTokenRequestProto request)
      throws ServiceException {
    return service.getDelegationToken(controller, request);
  }

  @Override
  public RenewDelegationTokenResponseProto renewDelegationToken(
      RpcController controller,
      RenewDelegationTokenRequestProto request)
      throws ServiceException {
    return service.renewDelegationToken(controller, request);
  }

  @Override
  public CancelDelegationTokenResponseProto cancelDelegationToken(
      RpcController controller,
      CancelDelegationTokenRequestProto request)
      throws ServiceException {
    return service.cancelDelegationToken(controller, request);
  }

  @Override
  public SetBalancerBandwidthResponseProto setBalancerBandwidth(
      RpcController controller,
      SetBalancerBandwidthRequestProto request)
      throws ServiceException {
    return service.setBalancerBandwidth(controller, request);
  }

  @Override
  public GetDataEncryptionKeyResponseProto getDataEncryptionKey(
      RpcController controller,
      GetDataEncryptionKeyRequestProto request)
      throws ServiceException {
    return service.getDataEncryptionKey(controller, request);
  }

  @Override
  public CreateSnapshotResponseProto createSnapshot(
      RpcController controller,
      CreateSnapshotRequestProto request)
      throws ServiceException {
    return service.createSnapshot(controller, request);
  }

  @Override
  public RenameSnapshotResponseProto renameSnapshot(
      RpcController controller,
      RenameSnapshotRequestProto request)
      throws ServiceException {
    return service.renameSnapshot(controller, request);
  }

  @Override
  public AllowSnapshotResponseProto allowSnapshot(
      RpcController controller,
      AllowSnapshotRequestProto request)
      throws ServiceException {
    return service.allowSnapshot(controller, request);
  }

  @Override
  public DisallowSnapshotResponseProto disallowSnapshot(
      RpcController controller,
      DisallowSnapshotRequestProto request)
      throws ServiceException {
    return service.disallowSnapshot(controller, request);
  }

  @Override
  public GetSnapshottableDirListingResponseProto getSnapshottableDirListing(
      RpcController controller,
      GetSnapshottableDirListingRequestProto request)
      throws ServiceException {
    return service.getSnapshottableDirListing(controller, request);
  }

  @Override
  public DeleteSnapshotResponseProto deleteSnapshot(
      RpcController controller,
      DeleteSnapshotRequestProto request)
      throws ServiceException {
    return service.deleteSnapshot(controller, request);
  }

  @Override
  public GetSnapshotDiffReportResponseProto getSnapshotDiffReport(
      RpcController controller,
      GetSnapshotDiffReportRequestProto request)
      throws ServiceException {
    return service.getSnapshotDiffReport(controller, request);
  }

  @Override
  public IsFileClosedResponseProto isFileClosed(
      RpcController controller,
      IsFileClosedRequestProto request)
      throws ServiceException {
    return service.isFileClosed(controller, request);
  }

  @Override
  public ModifyAclEntriesResponseProto modifyAclEntries(
      RpcController controller,
      ModifyAclEntriesRequestProto request)
      throws ServiceException {
    return service.modifyAclEntries(controller, request);
  }

  @Override
  public RemoveAclEntriesResponseProto removeAclEntries(
      RpcController controller,
      RemoveAclEntriesRequestProto request)
      throws ServiceException {
    return service.removeAclEntries(controller, request);
  }

  @Override
  public RemoveDefaultAclResponseProto removeDefaultAcl(
      RpcController controller,
      RemoveDefaultAclRequestProto request)
      throws ServiceException {
    return service.removeDefaultAcl(controller, request);
  }

  @Override
  public RemoveAclResponseProto removeAcl(
      RpcController controller,
      RemoveAclRequestProto request)
      throws ServiceException {
    return service.removeAcl(controller, request);
  }

  @Override
  public SetAclResponseProto setAcl(
      RpcController controller,
      SetAclRequestProto request)
      throws ServiceException {
    return service.setAcl(controller, request);
  }

  @Override
  public GetAclStatusResponseProto getAclStatus(
      RpcController controller,
      GetAclStatusRequestProto request)
      throws ServiceException {
    return service.getAclStatus(controller, request);
  }

  @Override
  public SetXAttrResponseProto setXAttr(
      RpcController controller,
      SetXAttrRequestProto request)
      throws ServiceException {
    return service.setXAttr(controller, request);
  }

  @Override
  public GetXAttrsResponseProto getXAttrs(
      RpcController controller,
      GetXAttrsRequestProto request)
      throws ServiceException {
    return service.getXAttrs(controller, request);
  }

  @Override
  public ListXAttrsResponseProto listXAttrs(
      RpcController controller,
      ListXAttrsRequestProto request)
      throws ServiceException {
    return service.listXAttrs(controller, request);
  }

  @Override
  public RemoveXAttrResponseProto removeXAttr(
      RpcController controller,
      RemoveXAttrRequestProto request)
      throws ServiceException {
    return service.removeXAttr(controller, request);
  }
}
