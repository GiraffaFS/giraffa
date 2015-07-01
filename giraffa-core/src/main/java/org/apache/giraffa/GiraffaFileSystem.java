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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.GiraffaClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

/**
 * Implementation of Giraffa FileSystem.
 * Giraffa stores its namespace in HBase table and retrieves data from
 * HDFS blocks, residing on DataNodes.
 */
public class GiraffaFileSystem extends FileSystem {
  GiraffaClient grfaClient;
  private URI hdfsUri;
  private URI hbaseUri;
  private Path workingDir;
  private URI uri;

  public GiraffaFileSystem() {
    // should be empty
  }

  @Override // FileSystem
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    // Not implemented
    throw new IOException("append: Implement me. It is not easy.");
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
                                   EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress,
                                   Options.ChecksumOpt checksumOpt)
      throws IOException {
    return new FSDataOutputStream(
        grfaClient.create(getPathName(f), permission, flags, replication,
            blockSize, progress, bufferSize, checksumOpt), statistics);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progressable)
      throws IOException {
    return create(f, permission,
        overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
            : EnumSet.of(CreateFlag.CREATE), bufferSize, replication,
        blockSize, progressable, new Options.ChecksumOpt(
            getServerDefaults(f).getChecksumType(),
            getServerDefaults(f).getBytesPerChecksum()));
  }

  @Override // FileSystem
  public boolean delete(Path f, boolean recursive) throws IOException {
    return grfaClient.delete(getPathName(f), recursive);
  }

  public static void format(GiraffaConfiguration conf,
                            boolean isConfirmationNeeded) throws IOException {
    if (isConfirmationNeeded) {
      System.err.print("Re-format Giraffa file system ? (Y or N) ");
      if (!(System.in.read() == 'Y')) {
        System.err.println("Format aborted.");
        return;
      }
      while(System.in.read() != '\n');
    }
    GiraffaClient.format(conf);

    GiraffaFileSystem grfs = null;
    try {
      grfs = (GiraffaFileSystem) FileSystem.get(conf);
      grfs.mkdirs(grfs.workingDir);
    } finally {
      IOUtils.cleanup(LOG, grfs);
    }
  }

  @Override // FileSystem
  public ContentSummary getContentSummary(Path f) throws IOException {
    HdfsFileStatus s = grfaClient.getFileInfo(getPathName(f));
    if (!s.isDir()) {
      // f is a file
      return new ContentSummary(s.getLen(), 1, 0, -1, s.getLen()*s.getReplication(), -1);
    }
    // f is a directory
    ContentSummary start = grfaClient.getContentSummary(getPathName(f));
    HdfsFileStatus[] list = grfaClient.listPaths(getPathName(f),
        HdfsFileStatus.EMPTY_NAME).getPartialListing();
    long[] summary = {0, 0, 1, start.getQuota(), 0, start.getSpaceQuota()};
    for(HdfsFileStatus t : list) {
      Path path = t.getFullPath(f).makeQualified(getUri(), getWorkingDirectory());
      // recursive if directory, else return file stats
      ContentSummary c = t.isDir() ? getContentSummary(path) :
          new ContentSummary(t.getLen(), 1, 0, -1, t.getLen()*t.getReplication(), -1);
      // compound results
      summary[0] += c.getLength();
      summary[1] += c.getFileCount();
      summary[2] += c.getDirectoryCount();
      summary[4] += c.getSpaceConsumed();
    }
    return new ContentSummary(summary[0], summary[1], summary[2],
        summary[3], summary[4], summary[5]);
  }

  @Override // FileSystem
  public FileStatus getFileStatus(Path f) throws IOException {
    HdfsFileStatus hdfsStatus = grfaClient.getFileInfo(getPathName(f));
    if (hdfsStatus == null) 
      throw new FileNotFoundException("File does not exist: " + f);
    return createFileStatus(hdfsStatus, f);
  }

  private FileStatus createFileStatus(HdfsFileStatus hdfsStatus, Path src) {
    return new FileStatus(hdfsStatus.getLen(), hdfsStatus.isDir(), hdfsStatus.getReplication(),
        hdfsStatus.getBlockSize(), hdfsStatus.getModificationTime(),
        hdfsStatus.getAccessTime(),
        hdfsStatus.getPermission(), hdfsStatus.getOwner(), hdfsStatus.getGroup(),
        (hdfsStatus.isSymlink() ? new Path(hdfsStatus.getSymlink()) : null),
        (hdfsStatus.getFullPath(src)).makeQualified(
            getUri(), getWorkingDirectory())); // fully-qualify path
  }

  URI getHBaseUri() {
    return hbaseUri;
  }

  URI getHDFSUri() {
    return hdfsUri;
  }

  @Override // FileSystem
  public URI getUri() {
    return uri;
  }

  @Override // FileSystem
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public String getScheme() {
    return GiraffaConfiguration.GRFA_URI_SCHEME;
  }

  @Override // FileSystem
  public void initialize(URI theUri, Configuration conf) throws IOException {
    GiraffaConfiguration grfaConf = conf instanceof GiraffaConfiguration ?
        (GiraffaConfiguration) conf : new GiraffaConfiguration(conf);
    
    super.initialize(theUri, grfaConf);

    // Initialize HDFS Client   --   SHV!!! Not used
    try {
      this.setHDFSUri(new URI(
          conf.get(GiraffaConfiguration.GRFA_HDFS_ADDRESS_KEY,
              GiraffaConfiguration.GRFA_HDFS_ADDRESS_DEFAULT)));
    } catch (URISyntaxException e) {
      throw new IOException("Incorrect URI for " +
          GiraffaConfiguration.GRFA_HDFS_ADDRESS_KEY, e);
    }

    // Initialize HBase Client   --   SHV!!! Not used
    try {
      this.setHBaseUri(new URI(
          conf.get(GiraffaConfiguration.GRFA_HBASE_ADDRESS_KEY,
              GiraffaConfiguration.GRFA_HBASE_ADDRESS_DEFAULT)));
    } catch (URISyntaxException e) {
      throw new IOException("Incorrect URI for " +
          GiraffaConfiguration.GRFA_HBASE_ADDRESS_KEY, e);
    }

    // closing
    this.uri = theUri;
    this.workingDir = makeQualified(new Path("/user/"
        + UserGroupInformation.getCurrentUser().getShortUserName()));

    grfaClient = new GiraffaClient(grfaConf, statistics);

    LOG.debug("uri = " + uri);
    LOG.debug("workingDir = " + workingDir);
  }

  private String getPathName(Path file) {
    return normalizePath(makeQualified(file).toUri().getPath());
  }

  static String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }

  @Override // FileSystem
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    String src = getPathName(f);

    // fetch the first batch of entries in the directory
    DirectoryListing thisListing = grfaClient.listPaths(
        src, HdfsFileStatus.EMPTY_NAME);
    
    if (thisListing == null) { // the directory does not exist
        throw new FileNotFoundException("File " + f + " does not exist.");
    }
    
    FileStatus[] fs = new FileStatus[thisListing.getPartialListing().length];
    for(int i = 0; i < fs.length; i++)
    {
      HdfsFileStatus hdfsStatus = thisListing.getPartialListing()[i];
      fs[i] = createFileStatus(hdfsStatus, f);
    }
    return fs;
  }

  @Override // FileSystem
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return grfaClient.mkdirs(getPathName(f), permission, true);
  }

  @Override // FileSystem
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return new FSDataInputStream(
        grfaClient.open(getPathName(f), bufferSize, true));
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return grfaClient.rename(getPathName(src), getPathName(dst));
  }

  @Override // FileSystem
  public void rename(Path src, Path dst, Options.Rename... options)
      throws IOException {
    grfaClient.rename(getPathName(src), getPathName(dst), options);
  }

  void setHBaseUri(URI hBaseUri) {
    hbaseUri = hBaseUri;
  }

  void setHDFSUri(URI hDFSUri) {
    hdfsUri = hDFSUri;
  }
  
  @Override // FileSystem
  public boolean setReplication(Path p, short replication) throws IOException {
    return grfaClient.setReplication(getPathName(p), replication);
  }
  
  @Override // FileSystem
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    grfaClient.setOwner(getPathName(p), username, groupname);
  }

  @Override // FileSystem
  public void setPermission(Path p, FsPermission permission)
  throws IOException {
    grfaClient.setPermission(getPathName(p), permission);
  }

  @Override // FileSystem
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    grfaClient.setTimes(getPathName(p), mtime, atime);
  }

  public void setQuota(Path src, long namespaceQuota, long diskspaceQuota) 
    throws IOException {
    grfaClient.setQuota(getPathName(src), namespaceQuota, diskspaceQuota);
  }

  @Override // FileSystem
  public void setWorkingDirectory(Path new_dir) {
    workingDir = makeQualified(
        new_dir.isAbsolute() ? new_dir :
          new Path(workingDir, new_dir));
    checkPath(workingDir);
  }

  @Override // FileSystem
  public void setXAttr(Path path, String name, byte[] value,
                       EnumSet<XAttrSetFlag> flag) throws IOException {
    grfaClient.setXAttr(getPathName(path), name, value, flag);
  }

  @Override // FileSystem
  public List<String> listXAttrs(Path path) throws IOException {
    return grfaClient.listXAttrs(getPathName(path));
  }

  @Override // FileSystem
  public byte[] getXAttr(Path path, String name) throws IOException {
    return grfaClient.getXAttr(getPathName(path), name);
  }

  @Override // FileSystem
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return grfaClient.getXAttrs(getPathName(path));
  }

  @Override // FileSystem
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
          throws IOException {
    return grfaClient.getXAttrs(getPathName(path), names);
  }

  @Override // FileSystem
  public void removeXAttr(Path path, String name) throws IOException {
    grfaClient.removeXAttr(getPathName(path), name);
  }

  @Override // FileSystem
  public void close() throws IOException {
    super.close();
    grfaClient.close();
  }

  public static void main(String argv[]) throws Exception {
    try {
      GiraffaConfiguration conf = new GiraffaConfiguration();
      if(argv.length > 0 && "format".equals(argv[0].toLowerCase()))
        GiraffaFileSystem.format(conf, true);
    } catch (Throwable e) {
      LOG.error(e);
      System.exit(-1);
    }
  }
}
