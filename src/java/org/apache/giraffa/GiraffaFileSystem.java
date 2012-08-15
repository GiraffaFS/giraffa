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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.GiraffaClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

/**
 * Implementation of Giraffa FileSystem.
 * Giraffa stores its namespace in HBase table and retrieves data from
 * HDFS blocks, residing on DataNodes.
 */
public class GiraffaFileSystem extends FileSystem {
  private GiraffaClient grfaClient;
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

  @Override // FileSystem
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return new FSDataOutputStream(
        grfaClient.create(getPathName(f), permission,
            overwrite ? EnumSet.of(CreateFlag.OVERWRITE) :
                        EnumSet.of(CreateFlag.CREATE),
                replication, blockSize, progress, bufferSize),
                statistics);
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

    GiraffaFileSystem grfs = (GiraffaFileSystem) FileSystem.get(conf);
    grfs.mkdirs(grfs.workingDir);
  }

  @Override // FileSystem
  public FileStatus getFileStatus(Path f) throws IOException {
    HdfsFileStatus hdfsFile = grfaClient.getFileInfo(getPathName(f));
    return createFileStatus(hdfsFile);
  }

  private FileStatus createFileStatus(HdfsFileStatus hdfsFile) {
    return new FileStatus(hdfsFile.getLen(), hdfsFile.isDir(),
        hdfsFile.getReplication(), hdfsFile.getBlockSize(),
        hdfsFile.getModificationTime(), hdfsFile.getAccessTime(),
        hdfsFile.getPermission(), hdfsFile.getOwner(), hdfsFile.getGroup(), 
        new Path(hdfsFile.getSymlink()), new Path(hdfsFile.getLocalName()));
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
    this.workingDir = new Path("/user/"
        + UserGroupInformation.getCurrentUser().getShortUserName());

    grfaClient = new GiraffaClient(grfaConf, statistics);

    LOG.debug("uri = " + uri);
    LOG.debug("workingDir = " + workingDir);
  }

  private String getPathName(Path file) {
    return makeQualified(file).toUri().getPath();
  }
  
  @Override // FileSystem
  public Path makeQualified(Path path) {
    checkPath(path);
    return path.makeQualified(this.getUri(), this.getWorkingDirectory());
  }

  @Override // FileSystem
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    String src = getPathName(f);

    // fetch the first batch of entries in the directory
    DirectoryListing thisListing = grfaClient.listPaths(
        src, HdfsFileStatus.EMPTY_NAME);
    
    FileStatus[] fs = new FileStatus[thisListing.getPartialListing().length];
    for(int i = 0; i < fs.length; i++)
    {
      HdfsFileStatus hd = thisListing.getPartialListing()[i];
      fs[i] = createFileStatus(hd);
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

  @Override // FileSystem
  public boolean rename(Path src, Path dst) throws IOException {
    // Not implemented
    throw new IOException("rename: Implement me. This is hard.");
  }

  void setHBaseUri(URI hBaseUri) {
    hbaseUri = hBaseUri;
  }

  void setHDFSUri(URI hDFSUri) {
    hdfsUri = hDFSUri;
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

  @Override // FileSystem
  public void setWorkingDirectory(Path new_dir) {
    workingDir = new_dir.isAbsolute() ? new_dir : new Path(workingDir, new_dir);
    checkPath(workingDir);
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
