package org.apache.giraffa.hbase;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.security.AccessControlException;

public interface NamespaceProtocol extends ClientProtocol {
  public static final long VERSION = 0L;

  // These are overrides of ClientProtocol, which are necessary due to
  // HBASE-6340.
  // Can be removed once jira is fixed.
  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src,
                                         long offset,
                                         long length) 
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public void create(String src, FsPermission masked, String clientName,
      EnumSetWritable<CreateFlag> flag, boolean createParent,
      short replication, long blockSize) throws AccessControlException,
      AlreadyBeingCreatedException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      IOException;

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public LocatedBlock addBlock(String src, String clientName,
      @Nullable ExtendedBlock previous, @Nullable DatanodeInfo[] excludeNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException;

  @Override // ClientProtocol
  public boolean complete(String src, String clientName, ExtendedBlock last)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException;

  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public DirectoryListing getListing(String src,
                                     byte[] startAfter,
                                     boolean needLocation)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public long getPreferredBlockSize(String filename) 
      throws IOException, UnresolvedLinkException;

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src) throws AccessControlException,
  FileNotFoundException, UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException, 
      UnresolvedLinkException, IOException;
}
