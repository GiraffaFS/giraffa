package org.apache.giraffa;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteResponseProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDelegationTokenResponseProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2RequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2ResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsResponseProto;
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
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

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
   * @throws IOException
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
      e.printStackTrace();
    }
  }

  @Override
  public void getServerDefaults(RpcController controller,
      GetServerDefaultsRequestProto req,
      RpcCallback<GetServerDefaultsResponseProto> done) {
    try {
      done.run(blockingTranslator.getServerDefaults(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void create(RpcController controller, CreateRequestProto req,
      RpcCallback<CreateResponseProto> done) {
    try {
      done.run(blockingTranslator.create(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void append(RpcController controller, AppendRequestProto req,
      RpcCallback<AppendResponseProto> done) {
    try {
      done.run(blockingTranslator.append(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setReplication(RpcController controller,
      SetReplicationRequestProto req,
      RpcCallback<SetReplicationResponseProto> done) {
    try {
      done.run(blockingTranslator.setReplication(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setPermission(RpcController controller,
      SetPermissionRequestProto req,
      RpcCallback<SetPermissionResponseProto> done) {
    try {
      done.run(blockingTranslator.setPermission(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setOwner(RpcController controller, SetOwnerRequestProto req,
      RpcCallback<SetOwnerResponseProto> done) {
    try {
      done.run(blockingTranslator.setOwner(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void
      abandonBlock(RpcController controller, AbandonBlockRequestProto req,
          RpcCallback<AbandonBlockResponseProto> done) {
    try {
      done.run(blockingTranslator.abandonBlock(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void addBlock(RpcController controller, AddBlockRequestProto req,
      RpcCallback<AddBlockResponseProto> done) {
    try {
      done.run(blockingTranslator.addBlock(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getAdditionalDatanode(RpcController controller,
      GetAdditionalDatanodeRequestProto req,
      RpcCallback<GetAdditionalDatanodeResponseProto> done) {
    try {
      done.run(blockingTranslator.getAdditionalDatanode(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void complete(RpcController controller, CompleteRequestProto req,
      RpcCallback<CompleteResponseProto> done) {
    try {
      done.run(blockingTranslator.complete(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void reportBadBlocks(RpcController controller,
      ReportBadBlocksRequestProto req,
      RpcCallback<ReportBadBlocksResponseProto> done) {
    try {
      done.run(blockingTranslator.reportBadBlocks(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void concat(RpcController controller, ConcatRequestProto req,
      RpcCallback<ConcatResponseProto> done) {
    try {
      done.run(blockingTranslator.concat(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void rename(RpcController controller, RenameRequestProto req,
      RpcCallback<RenameResponseProto> done) {
    try {
      done.run(blockingTranslator.rename(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void rename2(RpcController controller, Rename2RequestProto req,
      RpcCallback<Rename2ResponseProto> done) {
    try {
      done.run(blockingTranslator.rename2(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void delete(RpcController controller, DeleteRequestProto req,
      RpcCallback<DeleteResponseProto> done) {
    try {
      done.run(blockingTranslator.delete(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void mkdirs(RpcController controller, MkdirsRequestProto req,
      RpcCallback<MkdirsResponseProto> done) {
    try {
      done.run(blockingTranslator.mkdirs(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getListing(RpcController controller, GetListingRequestProto req,
      RpcCallback<GetListingResponseProto> done) {
    try {
      done.run(blockingTranslator.getListing(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void renewLease(RpcController controller, RenewLeaseRequestProto req,
      RpcCallback<RenewLeaseResponseProto> done) {
    try {
      done.run(blockingTranslator.renewLease(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void
      recoverLease(RpcController controller, RecoverLeaseRequestProto req,
          RpcCallback<RecoverLeaseResponseProto> done) {
    try {
      done.run(blockingTranslator.recoverLease(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getFsStats(RpcController controller, GetFsStatusRequestProto req,
      RpcCallback<GetFsStatsResponseProto> done) {
    try {
      done.run(blockingTranslator.getFsStats(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getDatanodeReport(RpcController controller,
      GetDatanodeReportRequestProto req,
      RpcCallback<GetDatanodeReportResponseProto> done) {
    try {
      done.run(blockingTranslator.getDatanodeReport(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getPreferredBlockSize(RpcController controller,
      GetPreferredBlockSizeRequestProto req,
      RpcCallback<GetPreferredBlockSizeResponseProto> done) {
    try {
      done.run(blockingTranslator.getPreferredBlockSize(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setSafeMode(RpcController controller,
      SetSafeModeRequestProto req, RpcCallback<SetSafeModeResponseProto> done) {
    try {
      done.run(blockingTranslator.setSafeMode(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void saveNamespace(RpcController controller,
      SaveNamespaceRequestProto req,
      RpcCallback<SaveNamespaceResponseProto> done) {
    try {
      done.run(blockingTranslator.saveNamespace(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void rollEdits(RpcController controller, RollEditsRequestProto req,
      RpcCallback<RollEditsResponseProto> done) {
    try {
      done.run(blockingTranslator.rollEdits(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void restoreFailedStorage(RpcController controller,
      RestoreFailedStorageRequestProto req,
      RpcCallback<RestoreFailedStorageResponseProto> done) {
    try {
      done.run(blockingTranslator.restoreFailedStorage(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void refreshNodes(RpcController controller,
      RefreshNodesRequestProto req,
      RpcCallback<RefreshNodesResponseProto> done) {
    try {
      done.run(blockingTranslator.refreshNodes(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void finalizeUpgrade(RpcController controller,
      FinalizeUpgradeRequestProto req,
      RpcCallback<FinalizeUpgradeResponseProto> done) {
    try {
      done.run(blockingTranslator.finalizeUpgrade(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void listCorruptFileBlocks(RpcController controller,
      ListCorruptFileBlocksRequestProto req,
      RpcCallback<ListCorruptFileBlocksResponseProto> done) {
    try {
      done.run(blockingTranslator.listCorruptFileBlocks(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void metaSave(RpcController controller, MetaSaveRequestProto req,
      RpcCallback<MetaSaveResponseProto> done) {
    try {
      done.run(blockingTranslator.metaSave(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getFileInfo(RpcController controller,
      GetFileInfoRequestProto req, RpcCallback<GetFileInfoResponseProto> done) {
    try {
      done.run(blockingTranslator.getFileInfo(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getFileLinkInfo(RpcController controller,
      GetFileLinkInfoRequestProto req,
      RpcCallback<GetFileLinkInfoResponseProto> done) {
    try {
      done.run(blockingTranslator.getFileLinkInfo(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getContentSummary(RpcController controller,
      GetContentSummaryRequestProto req,
      RpcCallback<GetContentSummaryResponseProto> done) {
    try {
      done.run(blockingTranslator.getContentSummary(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setQuota(RpcController controller, SetQuotaRequestProto req,
      RpcCallback<SetQuotaResponseProto> done) {
    try {
      done.run(blockingTranslator.setQuota(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void fsync(RpcController controller, FsyncRequestProto req,
      RpcCallback<FsyncResponseProto> done) {
    try {
      done.run(blockingTranslator.fsync(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setTimes(RpcController controller, SetTimesRequestProto req,
      RpcCallback<SetTimesResponseProto> done) {
    try {
      done.run(blockingTranslator.setTimes(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void createSymlink(RpcController controller,
      CreateSymlinkRequestProto req,
      RpcCallback<CreateSymlinkResponseProto> done) {
    try {
      done.run(blockingTranslator.createSymlink(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getLinkTarget(RpcController controller,
      GetLinkTargetRequestProto req,
      RpcCallback<GetLinkTargetResponseProto> done) {
    try {
      done.run(blockingTranslator.getLinkTarget(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void updateBlockForPipeline(RpcController controller,
      UpdateBlockForPipelineRequestProto req,
      RpcCallback<UpdateBlockForPipelineResponseProto> done) {
    try {
      done.run(blockingTranslator.updateBlockForPipeline(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void updatePipeline(RpcController controller,
      UpdatePipelineRequestProto req,
      RpcCallback<UpdatePipelineResponseProto> done) {
    try {
      done.run(blockingTranslator.updatePipeline(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getDelegationToken(RpcController controller,
      GetDelegationTokenRequestProto req,
      RpcCallback<GetDelegationTokenResponseProto> done) {
    try {
      done.run(blockingTranslator.getDelegationToken(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void renewDelegationToken(RpcController controller,
      RenewDelegationTokenRequestProto req,
      RpcCallback<RenewDelegationTokenResponseProto> done) {
    try {
      done.run(blockingTranslator.renewDelegationToken(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void cancelDelegationToken(RpcController controller,
      CancelDelegationTokenRequestProto req,
      RpcCallback<CancelDelegationTokenResponseProto> done) {
    try {
      done.run(blockingTranslator.cancelDelegationToken(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setBalancerBandwidth(RpcController controller,
      SetBalancerBandwidthRequestProto req,
      RpcCallback<SetBalancerBandwidthResponseProto> done) {
    try {
      done.run(blockingTranslator.setBalancerBandwidth(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getDataEncryptionKey(RpcController controller,
      GetDataEncryptionKeyRequestProto req,
      RpcCallback<GetDataEncryptionKeyResponseProto> done) {
    try {
      done.run(blockingTranslator.getDataEncryptionKey(controller, req));
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }
}
