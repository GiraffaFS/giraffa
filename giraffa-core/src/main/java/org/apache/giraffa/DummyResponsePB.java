package org.apache.giraffa;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;

/**
 * The HBase RPC implementation has a bug in which service calls that throw
 * exceptions must still return a buildable response, even though that
 * response will never be transmitted. This class contains static methods that
 * return fully initialized dummy response messages for each service call with
 * required response fields.
 */
public class DummyResponsePB {
  private final static LocatedBlockProto LOCATED_BLOCK_PROTO = PBHelper.convert(
      new LocatedBlock(new ExtendedBlock("", 0), new DatanodeInfo[0]));

  public static GetServerDefaultsResponseProto getServerDefaults() {
    return GetServerDefaultsResponseProto.newBuilder().setServerDefaults(
        PBHelper.convert(new FsServerDefaults())).build();
  }

  public static SetReplicationResponseProto setReplication() {
    return SetReplicationResponseProto.newBuilder().setResult(false).build();
  }

  public static AddBlockResponseProto addBlock() {
    return AddBlockResponseProto.newBuilder().setBlock(LOCATED_BLOCK_PROTO)
        .build();
  }

  public static GetAdditionalDatanodeResponseProto getAdditionalDatanode() {
    return GetAdditionalDatanodeResponseProto.newBuilder()
        .setBlock(LOCATED_BLOCK_PROTO).build();
  }

  public static CompleteResponseProto complete() {
    return CompleteResponseProto.newBuilder().setResult(false).build();
  }

  public static RenameResponseProto rename() {
    return RenameResponseProto.newBuilder().setResult(false).build();
  }

  public static DeleteResponseProto delete() {
    return DeleteResponseProto.newBuilder().setResult(false).build();
  }


  public static MkdirsResponseProto mkdirs() {
    return MkdirsResponseProto.newBuilder().setResult(false).build();
  }


  public static GetListingResponseProto getListing() {
    return GetListingResponseProto.newBuilder().setDirList(PBHelper.convert(
        new DirectoryListing(new HdfsFileStatus[0], 0))).build();
  }

  public static RecoverLeaseResponseProto recoverLease() {
    return RecoverLeaseResponseProto.newBuilder().setResult(false).build();
  }

  public static GetFsStatsResponseProto getFsStats() {
    return GetFsStatsResponseProto.newBuilder()
        .setCapacity(0)
        .setCorruptBlocks(0)
        .setMissingBlocks(0)
        .setRemaining(0)
        .setUnderReplicated(0)
        .setUsed(0).build();
  }

  public static GetPreferredBlockSizeResponseProto getPreferredBlockSize() {
    return GetPreferredBlockSizeResponseProto.newBuilder().setBsize(0).build();
  }


  public static SetSafeModeResponseProto setSafeMode() {
    return SetSafeModeResponseProto.newBuilder().setResult(false).build();
  }

  public static RollEditsResponseProto rollEdits() {
    return RollEditsResponseProto.newBuilder().setNewSegmentTxId(0).build();
  }


  public static RestoreFailedStorageResponseProto restoreFailedStorage() {
    return RestoreFailedStorageResponseProto.newBuilder().setResult(false)
        .build();
  }

  public static ListCorruptFileBlocksResponseProto listCorruptFileBlocks() {
    return ListCorruptFileBlocksResponseProto.newBuilder().setCorrupt(
        PBHelper.convert(new CorruptFileBlocks())).build();
  }

  public static AddCacheDirectiveResponseProto addCacheDirective() {
    return AddCacheDirectiveResponseProto.newBuilder().setId(0).build();
  }

  public static ListCacheDirectivesResponseProto listCacheDirectives() {
    return ListCacheDirectivesResponseProto.newBuilder().setHasMore(false)
        .build();
  }

  public static ListCachePoolsResponseProto listCachePools() {
    return ListCachePoolsResponseProto.newBuilder().setHasMore(false).build();
  }

  public static GetContentSummaryResponseProto getContentSummary() {
    return GetContentSummaryResponseProto.newBuilder().setSummary(
        PBHelper.convert(new ContentSummary())).build();
  }

  public static UpdateBlockForPipelineResponseProto updateBlockForPipeline() {
    return UpdateBlockForPipelineResponseProto.newBuilder()
        .setBlock(LOCATED_BLOCK_PROTO).build();
  }

  public static RenewDelegationTokenResponseProto renewDelegationToken() {
    return RenewDelegationTokenResponseProto.newBuilder().setNewExpiryTime(0)
        .build();
  }

  public static CreateSnapshotResponseProto createSnapshot() {
    return CreateSnapshotResponseProto.newBuilder().setSnapshotPath("").build();
  }

  public static GetSnapshotDiffReportResponseProto getSnapshotDiffReport() {
    return GetSnapshotDiffReportResponseProto.newBuilder().setDiffReport(
        PBHelper.convert(new SnapshotDiffReport("", "", "", null))).build();
  }

  public static IsFileClosedResponseProto isFileClosed() {
    return IsFileClosedResponseProto.newBuilder().setResult(false).build();
  }

  public static GetAclStatusResponseProto getAclStatus() {
    return PBHelper.convert(
        new AclStatus.Builder().owner("").owner("").build());
  }
}
