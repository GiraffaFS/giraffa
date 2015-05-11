package org.apache.giraffa.hbase;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.giraffa.DummyResponsePB;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
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
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;

import java.io.IOException;

/**
 * This class is used on the server side. Calls come across the wire for the
 * for protocol {@link ClientNamenodeProtocol.Interface}. This class uses
 * callbacks; for blocking services, see
 * {@link ClientNamenodeProtocolServerSideTranslatorPB}.
 * This class translates the PB data types to the native data types used inside
 * the NN as specified in the generic ClientProtocol.
 */
public class ClientNamenodeProtocolServerSideCallbackTranslatorPB
    implements ClientNamenodeProtocol.Interface {
  private ClientNamenodeProtocol.BlockingInterface blockingTranslator;

  /**
   * Constructor
   * 
   * @param server - the NN server
   */
  public ClientNamenodeProtocolServerSideCallbackTranslatorPB(
      ClientProtocol server) {
    try {
      this.blockingTranslator =
          new ClientNamenodeProtocolServerSideTranslatorPB(server);
    } catch (IOException e) { // will never happen
      e.printStackTrace();
    }
  }

  @Override
  public void getBlockLocations(RpcController controller,
      GetBlockLocationsRequestProto req,
      RpcCallback<GetBlockLocationsResponseProto> done) {
    try {
      done.run(blockingTranslator.getBlockLocations(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getServerDefaults(RpcController controller,
      GetServerDefaultsRequestProto req,
      RpcCallback<GetServerDefaultsResponseProto> done) {
    try {
      done.run(blockingTranslator.getServerDefaults(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.getServerDefaults());
    }
  }

  @Override
  public void create(RpcController controller, CreateRequestProto req,
      RpcCallback<CreateResponseProto> done) {
    try {
      done.run(blockingTranslator.create(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void append(RpcController controller, AppendRequestProto req,
      RpcCallback<AppendResponseProto> done) {
    try {
      done.run(blockingTranslator.append(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void setReplication(RpcController controller,
      SetReplicationRequestProto req,
      RpcCallback<SetReplicationResponseProto> done) {
    try {
      done.run(blockingTranslator.setReplication(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.setReplication());
    }
  }

  @Override
  public void setPermission(RpcController controller,
      SetPermissionRequestProto req,
      RpcCallback<SetPermissionResponseProto> done) {
    try {
      done.run(blockingTranslator.setPermission(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void setOwner(RpcController controller, SetOwnerRequestProto req,
      RpcCallback<SetOwnerResponseProto> done) {
    try {
      done.run(blockingTranslator.setOwner(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void abandonBlock(RpcController controller,
      AbandonBlockRequestProto req,
      RpcCallback<AbandonBlockResponseProto> done) {
    try {
      done.run(blockingTranslator.abandonBlock(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void addBlock(RpcController controller, AddBlockRequestProto req,
      RpcCallback<AddBlockResponseProto> done) {
    try {
      done.run(blockingTranslator.addBlock(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.addBlock());
    }
  }

  @Override
  public void getAdditionalDatanode(RpcController controller,
      GetAdditionalDatanodeRequestProto req,
      RpcCallback<GetAdditionalDatanodeResponseProto> done) {
    try {
      done.run(blockingTranslator.getAdditionalDatanode(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.getAdditionalDatanode());
    }
  }

  @Override
  public void complete(RpcController controller, CompleteRequestProto req,
      RpcCallback<CompleteResponseProto> done) {
    try {
      done.run(blockingTranslator.complete(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.complete());
    }
  }

  @Override
  public void reportBadBlocks(RpcController controller,
      ReportBadBlocksRequestProto req,
      RpcCallback<ReportBadBlocksResponseProto> done) {
    try {
      done.run(blockingTranslator.reportBadBlocks(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void concat(RpcController controller, ConcatRequestProto req,
      RpcCallback<ConcatResponseProto> done) {
    try {
      done.run(blockingTranslator.concat(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void rename(RpcController controller, RenameRequestProto req,
      RpcCallback<RenameResponseProto> done) {
    try {
      done.run(blockingTranslator.rename(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.rename());
    }
  }

  @Override
  public void rename2(RpcController controller, Rename2RequestProto req,
      RpcCallback<Rename2ResponseProto> done) {
    try {
      done.run(blockingTranslator.rename2(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void delete(RpcController controller, DeleteRequestProto req,
      RpcCallback<DeleteResponseProto> done) {
    try {
      done.run(blockingTranslator.delete(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.delete());
    }
  }

  @Override
  public void mkdirs(RpcController controller, MkdirsRequestProto req,
      RpcCallback<MkdirsResponseProto> done) {
    try {
      done.run(blockingTranslator.mkdirs(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.mkdirs());
    }
  }

  @Override
  public void getListing(RpcController controller, GetListingRequestProto req,
      RpcCallback<GetListingResponseProto> done) {
    try {
      done.run(blockingTranslator.getListing(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.getListing());
    }
  }

  @Override
  public void renewLease(RpcController controller, RenewLeaseRequestProto req,
      RpcCallback<RenewLeaseResponseProto> done) {
    try {
      done.run(blockingTranslator.renewLease(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void recoverLease(RpcController controller,
      RecoverLeaseRequestProto req,
      RpcCallback<RecoverLeaseResponseProto> done) {
    try {
      done.run(blockingTranslator.recoverLease(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.recoverLease());
    }
  }

  @Override
  public void getFsStats(RpcController controller, GetFsStatusRequestProto req,
      RpcCallback<GetFsStatsResponseProto> done) {
    try {
      done.run(blockingTranslator.getFsStats(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.getFsStats());
    }
  }

  @Override
  public void getDatanodeReport(RpcController controller,
      GetDatanodeReportRequestProto req,
      RpcCallback<GetDatanodeReportResponseProto> done) {
    try {
      done.run(blockingTranslator.getDatanodeReport(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getPreferredBlockSize(RpcController controller,
      GetPreferredBlockSizeRequestProto req,
      RpcCallback<GetPreferredBlockSizeResponseProto> done) {
    try {
      done.run(blockingTranslator.getPreferredBlockSize(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.getPreferredBlockSize());
    }
  }

  @Override
  public void setSafeMode(RpcController controller,
      SetSafeModeRequestProto req, RpcCallback<SetSafeModeResponseProto> done) {
    try {
      done.run(blockingTranslator.setSafeMode(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.setSafeMode());
    }
  }

  @Override
  public void saveNamespace(RpcController controller,
      SaveNamespaceRequestProto req,
      RpcCallback<SaveNamespaceResponseProto> done) {
    try {
      done.run(blockingTranslator.saveNamespace(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void rollEdits(RpcController controller, RollEditsRequestProto req,
      RpcCallback<RollEditsResponseProto> done) {
    try {
      done.run(blockingTranslator.rollEdits(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.rollEdits());
    }
  }

  @Override
  public void restoreFailedStorage(RpcController controller,
      RestoreFailedStorageRequestProto req,
      RpcCallback<RestoreFailedStorageResponseProto> done) {
    try {
      done.run(blockingTranslator.restoreFailedStorage(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.restoreFailedStorage());
    }
  }

  @Override
  public void refreshNodes(RpcController controller,
      RefreshNodesRequestProto req,
      RpcCallback<RefreshNodesResponseProto> done) {
    try {
      done.run(blockingTranslator.refreshNodes(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void finalizeUpgrade(RpcController controller,
      FinalizeUpgradeRequestProto req,
      RpcCallback<FinalizeUpgradeResponseProto> done) {
    try {
      done.run(blockingTranslator.finalizeUpgrade(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void rollingUpgrade(RpcController controller,
                             RollingUpgradeRequestProto req,
                             RpcCallback<RollingUpgradeResponseProto> done) {
    try {
      done.run(blockingTranslator.rollingUpgrade(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void listCorruptFileBlocks(RpcController controller,
      ListCorruptFileBlocksRequestProto req,
      RpcCallback<ListCorruptFileBlocksResponseProto> done) {
    try {
      done.run(blockingTranslator.listCorruptFileBlocks(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.listCorruptFileBlocks());
    }
  }

  @Override
  public void metaSave(RpcController controller, MetaSaveRequestProto req,
      RpcCallback<MetaSaveResponseProto> done) {
    try {
      done.run(blockingTranslator.metaSave(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getFileInfo(RpcController controller,
      GetFileInfoRequestProto req, RpcCallback<GetFileInfoResponseProto> done) {
    try {
      done.run(blockingTranslator.getFileInfo(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void addCacheDirective(
      RpcController controller,
      AddCacheDirectiveRequestProto req,
      RpcCallback<AddCacheDirectiveResponseProto> done) {
    try {
      done.run(blockingTranslator.addCacheDirective(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.addCacheDirective());
    }
  }

  @Override
  public void modifyCacheDirective(
      RpcController controller,
      ModifyCacheDirectiveRequestProto req,
      RpcCallback<ModifyCacheDirectiveResponseProto> done) {
    try {
      done.run(blockingTranslator.modifyCacheDirective(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void removeCacheDirective(
      RpcController controller,
      RemoveCacheDirectiveRequestProto req,
      RpcCallback<RemoveCacheDirectiveResponseProto> done) {
    try {
      done.run(blockingTranslator.removeCacheDirective(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void listCacheDirectives(
      RpcController controller,
      ListCacheDirectivesRequestProto req,
      RpcCallback<ListCacheDirectivesResponseProto> done) {
    try {
      done.run(blockingTranslator.listCacheDirectives(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.listCacheDirectives());
    }
  }

  @Override
  public void addCachePool(RpcController controller,
                           AddCachePoolRequestProto req,
                           RpcCallback<AddCachePoolResponseProto> done) {
    try {
      done.run(blockingTranslator.addCachePool(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void modifyCachePool(RpcController controller,
                              ModifyCachePoolRequestProto req,
                              RpcCallback<ModifyCachePoolResponseProto> done) {
    try {
      done.run(blockingTranslator.modifyCachePool(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void removeCachePool(RpcController controller,
                              RemoveCachePoolRequestProto req,
                              RpcCallback<RemoveCachePoolResponseProto> done) {
    try {
      done.run(blockingTranslator.removeCachePool(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void listCachePools(RpcController controller,
                             ListCachePoolsRequestProto req,
                             RpcCallback<ListCachePoolsResponseProto> done) {
    try {
      done.run(blockingTranslator.listCachePools(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.listCachePools());
    }
  }

  @Override
  public void getFileLinkInfo(RpcController controller,
      GetFileLinkInfoRequestProto req,
      RpcCallback<GetFileLinkInfoResponseProto> done) {
    try {
      done.run(blockingTranslator.getFileLinkInfo(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getContentSummary(RpcController controller,
      GetContentSummaryRequestProto req,
      RpcCallback<GetContentSummaryResponseProto> done) {
    try {
      done.run(blockingTranslator.getContentSummary(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.getContentSummary());
    }
  }

  @Override
  public void setQuota(RpcController controller, SetQuotaRequestProto req,
      RpcCallback<SetQuotaResponseProto> done) {
    try {
      done.run(blockingTranslator.setQuota(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void fsync(RpcController controller, FsyncRequestProto req,
      RpcCallback<FsyncResponseProto> done) {
    try {
      done.run(blockingTranslator.fsync(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void setTimes(RpcController controller, SetTimesRequestProto req,
      RpcCallback<SetTimesResponseProto> done) {
    try {
      done.run(blockingTranslator.setTimes(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void createSymlink(RpcController controller,
      CreateSymlinkRequestProto req,
      RpcCallback<CreateSymlinkResponseProto> done) {
    try {
      done.run(blockingTranslator.createSymlink(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getLinkTarget(RpcController controller,
      GetLinkTargetRequestProto req,
      RpcCallback<GetLinkTargetResponseProto> done) {
    try {
      done.run(blockingTranslator.getLinkTarget(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void updateBlockForPipeline(RpcController controller,
      UpdateBlockForPipelineRequestProto req,
      RpcCallback<UpdateBlockForPipelineResponseProto> done) {
    try {
      done.run(blockingTranslator.updateBlockForPipeline(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.updateBlockForPipeline());
    }
  }

  @Override
  public void updatePipeline(RpcController controller,
      UpdatePipelineRequestProto req,
      RpcCallback<UpdatePipelineResponseProto> done) {
    try {
      done.run(blockingTranslator.updatePipeline(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getDelegationToken(RpcController controller,
      GetDelegationTokenRequestProto req,
      RpcCallback<GetDelegationTokenResponseProto> done) {
    try {
      done.run(blockingTranslator.getDelegationToken(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void renewDelegationToken(RpcController controller,
      RenewDelegationTokenRequestProto req,
      RpcCallback<RenewDelegationTokenResponseProto> done) {
    try {
      done.run(blockingTranslator.renewDelegationToken(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.renewDelegationToken());
    }
  }

  @Override
  public void cancelDelegationToken(RpcController controller,
      CancelDelegationTokenRequestProto req,
      RpcCallback<CancelDelegationTokenResponseProto> done) {
    try {
      done.run(blockingTranslator.cancelDelegationToken(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void setBalancerBandwidth(RpcController controller,
      SetBalancerBandwidthRequestProto req,
      RpcCallback<SetBalancerBandwidthResponseProto> done) {
    try {
      done.run(blockingTranslator.setBalancerBandwidth(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getDataEncryptionKey(RpcController controller,
      GetDataEncryptionKeyRequestProto req,
      RpcCallback<GetDataEncryptionKeyResponseProto> done) {
    try {
      done.run(blockingTranslator.getDataEncryptionKey(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void createSnapshot(RpcController controller,
                             CreateSnapshotRequestProto req,
                             RpcCallback<CreateSnapshotResponseProto> done) {
    try {
      done.run(blockingTranslator.createSnapshot(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.createSnapshot());
    }
  }

  @Override
  public void renameSnapshot(RpcController controller,
                             RenameSnapshotRequestProto req,
                             RpcCallback<RenameSnapshotResponseProto> done) {
    try {
      done.run(blockingTranslator.renameSnapshot(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void allowSnapshot(RpcController controller,
                            AllowSnapshotRequestProto req,
                            RpcCallback<AllowSnapshotResponseProto> done) {
    try {
      done.run(blockingTranslator.allowSnapshot(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void disallowSnapshot(
      RpcController controller,
      DisallowSnapshotRequestProto req,
      RpcCallback<DisallowSnapshotResponseProto> done) {
    try {
      done.run(blockingTranslator.disallowSnapshot(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getSnapshottableDirListing(
      RpcController controller,
      GetSnapshottableDirListingRequestProto req,
      RpcCallback<GetSnapshottableDirListingResponseProto> done) {
    try {
      done.run(blockingTranslator.getSnapshottableDirListing(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void deleteSnapshot(RpcController controller,
                             DeleteSnapshotRequestProto req,
                             RpcCallback<DeleteSnapshotResponseProto> done) {
    try {
      done.run(blockingTranslator.deleteSnapshot(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getSnapshotDiffReport(
      RpcController controller,
      GetSnapshotDiffReportRequestProto req,
      RpcCallback<GetSnapshotDiffReportResponseProto> done) {
    try {
      done.run(blockingTranslator.getSnapshotDiffReport(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.getSnapshotDiffReport());
    }
  }

  @Override
  public void isFileClosed(RpcController controller,
                           IsFileClosedRequestProto req,
                           RpcCallback<IsFileClosedResponseProto> done) {
    try {
      done.run(blockingTranslator.isFileClosed(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.isFileClosed());
    }
  }

  @Override
  public void modifyAclEntries(
      RpcController controller,
      ModifyAclEntriesRequestProto req,
      RpcCallback<ModifyAclEntriesResponseProto> done) {
    try {
      done.run(blockingTranslator.modifyAclEntries(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void removeAclEntries(
      RpcController controller,
      RemoveAclEntriesRequestProto req,
      RpcCallback<RemoveAclEntriesResponseProto> done) {
    try {
      done.run(blockingTranslator.removeAclEntries(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void removeDefaultAcl(
      RpcController controller,
      RemoveDefaultAclRequestProto req,
      RpcCallback<RemoveDefaultAclResponseProto> done) {
    try {
      done.run(blockingTranslator.removeDefaultAcl(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void removeAcl(RpcController controller,
                        RemoveAclRequestProto req,
                        RpcCallback<RemoveAclResponseProto> done) {
    try {
      done.run(blockingTranslator.removeAcl(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void setAcl(RpcController controller,
                     SetAclRequestProto req,
                     RpcCallback<SetAclResponseProto> done) {
    try {
      done.run(blockingTranslator.setAcl(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getAclStatus(RpcController controller,
                           GetAclStatusRequestProto req,
                           RpcCallback<GetAclStatusResponseProto> done) {
    try {
      done.run(blockingTranslator.getAclStatus(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done,
          DummyResponsePB.getAclStatus());
    }
  }

  @Override
  public void setXAttr(RpcController controller,
                       SetXAttrRequestProto req,
                       RpcCallback<SetXAttrResponseProto> done) {
    try {
      done.run(blockingTranslator.setXAttr(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void getXAttrs(RpcController controller,
                        GetXAttrsRequestProto req,
                        RpcCallback<GetXAttrsResponseProto> done) {
    try {
      done.run(blockingTranslator.getXAttrs(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void listXAttrs(RpcController controller,
                         ListXAttrsRequestProto req,
                         RpcCallback<ListXAttrsResponseProto> done) {
    try {
      done.run(blockingTranslator.listXAttrs(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  @Override
  public void removeXAttr(RpcController controller,
                          RemoveXAttrRequestProto req,
                          RpcCallback<RemoveXAttrResponseProto> done) {
    try {
      done.run(blockingTranslator.removeXAttr(controller, req));
    } catch (ServiceException e) {
      handleRemoteException(controller, e, done);
    }
  }

  private static void handleRemoteException(RpcController controller,
      ServiceException e, RpcCallback<?> done) {
    handleRemoteException(controller, e, done, null);
  }

  private static <T> void handleRemoteException(RpcController controller,
      ServiceException e, RpcCallback<T> done, T response) {
    ResponseConverter.setControllerException(controller,
        ProtobufUtil.getRemoteException(e));
    done.run(response);
  }
}
