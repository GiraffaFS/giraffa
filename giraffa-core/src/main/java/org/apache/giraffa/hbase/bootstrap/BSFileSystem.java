package org.apache.giraffa.hbase.bootstrap;

import static org.apache.giraffa.GiraffaConfiguration.GRFA_BLOCK_MANAGER_ADDRESS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.util.Time.now;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraffa.GiraffaConstants;
import org.apache.giraffa.GiraffaFileSystem;
import org.apache.giraffa.hbase.BlockManagementAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class BSFileSystem extends GiraffaFileSystem {
  private Map<String, Pair<SFileStatus, byte[]>> bootstrapMap;
  private volatile boolean isBootstrapped;
  private String bmAddress;

  public BSFileSystem() throws IOException {
    // should be empty
  }

  @Override // FileSystem
  public synchronized void initialize(URI name, Configuration conf)
      throws IOException {
    super.initialize(name, conf);
    // read super block
    bootstrapMap = readSuperBlock(conf);
    bmAddress = conf.get(GRFA_BLOCK_MANAGER_ADDRESS_KEY);
    assert bmAddress != null : "bmAddress is null";
    return;
  }

  @Override // FileSystem
  public synchronized FileStatus getFileStatus(Path f) throws IOException {
    if(isBootstrapped) return super.getFileStatus(f);
    String src = f.toUri().getPath();
    Pair<? extends FileStatus, byte[]> file = bootstrapMap.get(src);
    if(file != null)
      return file.getFirst();
    // try to reread super block
    if(f.toUri().getPath().contains("amespace")) {  // SHV !!! hacky
      bootstrapMap = readSuperBlock(bmAddress);
      file = bootstrapMap.get(src);
      if(file != null)
        return file.getFirst();
    }
    String msg = "bootstrapMap does not contain " + src;
    LOG.debug(msg);
    throw new FileNotFoundException(msg);
  }

  @Override // FileSystem
  public synchronized FileStatus[] listStatus(Path f)
      throws FileNotFoundException, IOException {
    if(isBootstrapped) return super.listStatus(f);
    FileStatus status = getFileStatus(f);
    if(status == null)
        return new FileStatus[] {};
    if(status.isFile())
      return new FileStatus[] {status};
    // get directory listing
    String src = f.toUri().getPath();
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    for(Entry<String, Pair<SFileStatus, byte[]>> e : bootstrapMap.entrySet()) {
      FileStatus cur = e.getValue().getFirst();
      Path parent = cur.getPath().getParent();
      if(parent != null && parent.toUri().getPath().equals(src))
        results.add(cur);
    }
    return results.toArray(new FileStatus[]{});
  }

  @Override // FileSystem
  public synchronized FSDataInputStream open(Path f, int bufferSize)
      throws IOException {
    if(isBootstrapped) return super.open(f, bufferSize);
    String src = f.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootstrapMap.get(src);
    if(file == null) {
      String msg = "bootstrapMap does not contain " + src;
      LOG.debug(msg);
      throw new FileNotFoundException(msg);
    }
    return new FSDataInputStream(
        new StaticByteInputStream(file.getSecond()));
  }

  @Override // FileSystem
  public synchronized boolean mkdirs(Path f, FsPermission permission)
      throws IOException {
    if(isBootstrapped) return super.mkdirs(f, permission);
    for(Path current = f;
        current != null && ! exists(current);
        current = current.getParent()) {
      long time = now();
      SFileStatus status = new SFileStatus(0L, true, 0, 0L, time, time,
          permission, "giraffa", "giraffa", null, current);
      String src = current.toUri().getPath();
      bootstrapMap.put(src, new Pair<SFileStatus, byte[]>(status, null));
    }
    return true;
  }

  @Override // FileSystem
  public synchronized FSDataOutputStream create(Path f,
      FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress)
          throws IOException {
    if(isBootstrapped)
      return super.create(f, permission, overwrite,
          bufferSize, replication, blockSize, progress);
    mkdirs(f.getParent());
    String src = f.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootstrapMap.get(src);
    SFileStatus status = null;
    if(file != null) {
      if(!overwrite)
        throw new FileAlreadyExistsException("File already exists: " + src);
      status = file.getFirst();
    } else {
      long time = now();
      status = new SFileStatus(0L, false, replication, blockSize, time, time,
          permission, "giraffa", "giraffa", null, f);
    }
    bootstrapMap.put(src, new Pair<SFileStatus, byte[]>(status, null));
    return new FSDataOutputStream( new BSFSOutputStream(src), null);
  }

  @Override // FileSystem
  public FSDataOutputStream createNonRecursive(Path f,
      FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress)
          throws IOException {
    return create(f, permission, overwrite,
                  bufferSize, replication, blockSize, progress);
  }

  @Override // FileSystem
  public synchronized boolean rename(Path src, Path dst) throws IOException {
    if(isBootstrapped)
      return super.rename(src, dst);
    String s = src.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootstrapMap.remove(s);
    if(file == null) {
      // try to reread super block - SHV!!!
      bootstrapMap = readSuperBlock(bmAddress);
      file = bootstrapMap.remove(s);
      if(file == null)
        throw new FileNotFoundException(
            "rename: bootstrapMap does not contain " + src);
    }
    if(file.getFirst().isDirectory())
      renameRecursive(file, dst);
    else {
      file.getFirst().setPath(dst);
      bootstrapMap.put(dst.toUri().getPath(), file);
    }
    writeSuperBlock(bmAddress, bootstrapMap);
    LOG.debug("Bootstrap rename from: " + src + " to " + dst);
    return true;
  }

  /**
   * Recursively rename all children of dir to the new parent dst.
   */
  private void renameRecursive(Pair<SFileStatus, byte[]> dir, Path dst) {
    String srcDir = dir.getFirst().getPath().toUri().getPath();
    String dstDir = dst.toUri().getPath();
    Iterator<Entry<String, Pair<SFileStatus, byte[]>>> entries =
        bootstrapMap.entrySet().iterator();
    Map<String, Pair<SFileStatus, byte[]>> renamed =
        new HashMap<String, Pair<SFileStatus, byte[]>>();
    while(entries.hasNext()) {
      Pair<SFileStatus, byte[]> cur = entries.next().getValue();
      Path curPath = cur.getFirst().getPath();
      if(curPath == null || !curPath.toUri().getPath().startsWith(srcDir))
        continue;
      entries.remove();
      String curS = curPath.toUri().getPath();
      String d = curS.replaceFirst(srcDir, dstDir);
      cur.getFirst().setPath(new Path(curPath.toUri().getScheme(),
                                      curPath.toUri().getAuthority(), d));
      renamed.put(d, cur);
    }
    bootstrapMap.putAll(renamed);
    dir.getFirst().setPath(dst);
    bootstrapMap.put(dstDir, dir);
  }

  @Override // FileSystem
  public synchronized boolean delete(Path f, boolean recursive)
      throws IOException {
    if(isBootstrapped)
      return super.delete(f, recursive);
    String src = f.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootstrapMap.remove(src);
    return file != null;
  }

  @Override // FileSystem
  public synchronized void setTimes(Path p, long mtime, long atime)
      throws IOException {
    if(isBootstrapped)
      super.setTimes(p, mtime, atime);
    String src = p.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootstrapMap.get(src);
    if(file == null)
      throw new FileNotFoundException("bootstrapMap does not contain " + src);
    FileStatus oldStatus = file.getFirst();
    // Update times
    SFileStatus status = new SFileStatus(
        oldStatus.getLen(), oldStatus.isDirectory(),
        oldStatus.getReplication(), oldStatus.getBlockSize(),
        (mtime < 0 ? oldStatus.getModificationTime() : mtime),
        (atime < 0 ? oldStatus.getAccessTime() : atime),
        oldStatus.getPermission(), oldStatus.getOwner(), oldStatus.getGroup(),
        null, oldStatus.getPath());
    file.setFirst(status);
    bootstrapMap.put(src, file);
  }

  public synchronized void writeSuperBlock(Configuration conf)
          throws IOException {
    writeSuperBlock(conf, bootstrapMap);
    LOG.info("Super block contents: ");
    LOG.info(bootstrapMap.keySet().toString());
  }

  public synchronized void finalizeBootstrap() throws IOException {
    isBootstrapped = true;
    LOG.info("Bootstrap is finalized.");
  }

  public synchronized void copyBootstrap2Giraffa(Path[] paths)
      throws IOException {
    LOG.debug(this.toString());
    for(Entry<String, Pair<SFileStatus, byte[]>> e : bootstrapMap.entrySet()) {
      FileStatus stat = e.getValue().getFirst();
      Path curPath = stat.getPath();
      FsPermission curPermission = stat.getPermission();
      LOG.debug("Copy: " + stat);
      if(stat.isDirectory() && !stat.getPath().isRoot()) {
        giraffaMkdirs(curPath, curPermission);
      } else if(stat.isFile()) {
        InputStream in=null;
        OutputStream out = null;
        try {
          in = open(curPath, IO_FILE_BUFFER_SIZE_DEFAULT);
          out = giraffaCreate(curPath, curPermission, false,
              IO_FILE_BUFFER_SIZE_DEFAULT,
              DFS_REPLICATION_DEFAULT, DFS_BLOCK_SIZE_DEFAULT, null);
          IOUtils.copyBytes(in, out, IO_FILE_BUFFER_SIZE_DEFAULT, true);
        } finally {
          IOUtils.closeStream(out);
          IOUtils.closeStream(in);
        }
      }
    }
  }

  public static
  Map<String, Pair<SFileStatus, byte[]>> readSuperBlock(Configuration conf)
      throws IOException {
    BSFSConfiguration bsConf = ((conf instanceof BSFSConfiguration) ?
        (BSFSConfiguration)conf : new BSFSConfiguration(conf));
    String bmAddress = bsConf.get(GRFA_BLOCK_MANAGER_ADDRESS_KEY);
    assert bmAddress != null : "bmAddress is null";
    return readSuperBlock(bmAddress);
  }

  @SuppressWarnings("unchecked")
  public static
  Map<String, Pair<SFileStatus, byte[]>> readSuperBlock(String bmAddress)
      throws IOException {
    Configuration hdfsConf = new Configuration();
    hdfsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    FileSystem hdfs = FileSystem.get(new Path(bmAddress).toUri(), hdfsConf);

    // check if super block exists
    Path sbPath = getGiraffaSuperBlockPath();
    Map<String, Pair<SFileStatus, byte[]>> map =
        new HashMap<String, Pair<SFileStatus, byte[]>>();
    if(!hdfs.exists(sbPath)) {
      LOG.info("Super block does not exist yet: " + sbPath);
      return map;
    }

    // read super block
    FSDataInputStream hdfsStream = hdfs.open(sbPath);
    ObjectInputStream in = new ObjectInputStream(hdfsStream);
    try {
      map = (HashMap<String, Pair<SFileStatus, byte[]>>) in.readObject();
    } catch (Exception e) {
      throw new IOException("Cannot read super block.", e);
    } finally {
      in.close();
    }
    LOG.info("Read super block: " + sbPath);
    return map;
  }

  public static void writeSuperBlock(Configuration conf,
                                     Map<String, Pair<SFileStatus, byte[]>> map)
      throws IOException {
    String bmAddress = conf.get(GRFA_BLOCK_MANAGER_ADDRESS_KEY);
    assert bmAddress != null : "bmAddress is null";
    writeSuperBlock(bmAddress, map);
  }

  public static void writeSuperBlock(String bmAddress,
      Map<String, Pair<SFileStatus, byte[]>> map) throws IOException {
    FileSystem hdfs = FileSystem.get(new Path(bmAddress).toUri(), new Configuration());
    Path sbPath = getGiraffaSuperBlockPath();
    // create new super block
    FSDataOutputStream sbOut = hdfs.create(sbPath, true);
    // read super block
    ObjectOutputStream out = new ObjectOutputStream(sbOut);
    try {
      out.writeObject(map);
    } catch (Exception e) {
      throw new IOException("Cannot write super block.", e);
    } finally {
      out.close();
    }
    LOG.info("Write new super block: " + sbPath
        + " (" + sbOut.size() + " bytes)");
  }

  public static Path getGiraffaSuperBlockPath() {
    ExtendedBlock sb =
        new ExtendedBlock("", GiraffaConstants.GRFA_SUPER_BLOCK_ID);
    return BlockManagementAgent.getGiraffaBlockPath(sb);
  }

  class BSFSOutputStream extends OutputStream {
    private String src;

    public BSFSOutputStream(String src) throws IOException {
      this.src = src;
    }

    @Override
    public void write(int b) throws IOException {
      write(new byte[]{(byte)b}, 0, 1);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      // LOG.debug("Write to " + src + " length = " + b.length);
      synchronized(BSFileSystem.this) {
        Pair<SFileStatus, byte[]> file = bootstrapMap.get(src);
        if(file == null)
          throw new FileNotFoundException("bootstrapMap does not have " + src);
        FileStatus oldStatus = file.getFirst();
        // Update file length
        SFileStatus status = new SFileStatus(b.length, false,
            oldStatus.getReplication(), oldStatus.getBlockSize(),
            oldStatus.getModificationTime(), oldStatus.getAccessTime(),
            oldStatus.getPermission(), oldStatus.getOwner(),
            oldStatus.getGroup(), null, oldStatus.getPath());
        file.setFirst(status);
        // Update file context
        byte[] oldBytes = file.getSecond();
        byte[] bytes = null;
        if(oldBytes == null) {
          bytes = b;
        } else {
          bytes = new byte[oldBytes.length + b.length];
          System.arraycopy(oldBytes, 0, bytes, 0, oldBytes.length);
          System.arraycopy(b, 0, bytes, oldBytes.length, b.length);
        }
        file.setSecond(bytes);
        bootstrapMap.put(src, file);
      }
    }

    public void close() throws IOException {
      synchronized(BSFileSystem.this) {
        Pair<SFileStatus, byte[]> file = bootstrapMap.get(src);
        if(file.getSecond() == null) {  // empty file handling
          file.setSecond(new byte[]{});
          bootstrapMap.put(src, file);
        }
        writeSuperBlock(bmAddress, bootstrapMap);
      }
      LOG.debug("Bootstrap close for: " + src);
    }
  }

  /**
   * Serializable FileStatus.
   * SHV!!! Should use protobuf to serialize the entire bootstrapMap.
   */
  static class SFileStatus extends FileStatus implements Serializable {
    private static final long serialVersionUID = -8797167960711551287L;

    public SFileStatus(long length, boolean isdir,
        int replication,
        long blocksize, long mtime, long atime,
        FsPermission permission, String owner, String group,
        Path symlink,
        Path path) {
      super(length, isdir, replication, blocksize, mtime, atime,
            permission, owner, group, symlink, path);
    }

    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      this.readFields(in);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      this.write(out);
    }
  }

  public void giraffaMkdirs(Path f, FsPermission permission)
      throws IOException {
    super.mkdirs(f, permission);
  }

  public FSDataOutputStream giraffaCreate(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    return super.create(f, permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }

  @Override // Object
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append(" contents ------------\n");
    // print map entries
    for(Entry<String, Pair<SFileStatus, byte[]>> e : bootstrapMap.entrySet())
      sb.append("key = ").append(e.getKey()).append("\n")
        .append(e.getValue().getFirst()).append("\n");
    return sb.toString();
  }
}
