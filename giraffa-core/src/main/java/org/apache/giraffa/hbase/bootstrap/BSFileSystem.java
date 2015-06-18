package org.apache.giraffa.hbase.bootstrap;

import static org.apache.giraffa.GiraffaConfiguration.GRFA_BLOCK_MANAGER_ADDRESS_KEY;
import static org.apache.hadoop.util.Time.now;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.hadoop.hbase.master.HMasterGiraffa.BSFSConfiguration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.util.Progressable;

public class BSFileSystem extends GiraffaFileSystem {
  private Map<String, Pair<SFileStatus, byte[]>> bootStrapMap;
  private volatile boolean isBootStrapped;

  public BSFileSystem() throws IOException {
    // should be empty
  }

  @Override // FileSystem
  public synchronized void initialize(URI name, Configuration conf)
      throws IOException {
    super.initialize(name, conf);
    // read super block
    bootStrapMap = readSuperBlock(conf);
    return;
  }

  @Override // FileSystem
  public synchronized FileStatus getFileStatus(Path f) throws IOException {
    if(isBootStrapped) return super.getFileStatus(f);
    String src = f.toUri().getPath();
    Pair<? extends FileStatus, byte[]> file = bootStrapMap.get(src);
    if(file == null) {
      String msg = "bootStrapMap does not contain " + src;
      LOG.debug(msg);
      throw new FileNotFoundException(msg);
    }
    return file.getFirst();
  }

  @Override // FileSystem
  public synchronized FileStatus[] listStatus(Path f)
      throws FileNotFoundException, IOException {
    if(isBootStrapped) return super.listStatus(f);
    FileStatus status = getFileStatus(f);
    if(status == null)
      return new FileStatus[] {};
    if(status.isFile())
      return new FileStatus[] {status};
    // get directory listing
    String src = f.toUri().getPath();
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    for(Entry<String, Pair<SFileStatus, byte[]>> e : bootStrapMap.entrySet()) {
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
    if(isBootStrapped) return super.open(f, bufferSize);
    String src = f.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootStrapMap.get(src);
    if(file == null) {
      String msg = "bootStrapMap does not contain " + src;
      LOG.debug(msg);
      throw new FileNotFoundException(msg);
    }
    return new FSDataInputStream(
        new StaticByteInputStream(file.getSecond()));
  }

  @Override // FileSystem
  public synchronized boolean mkdirs(Path f, FsPermission permission)
      throws IOException {
    if(isBootStrapped) return super.mkdirs(f, permission);
    for(Path current = f;
        current != null && ! exists(current);
        current = current.getParent()) {
      long time = now();
      SFileStatus status = new SFileStatus(0L, true, 0, 0L, time, time,
          permission, "giraffa", "giraffa", null, current);
      String src = current.toUri().getPath();
      bootStrapMap.put(src, new Pair<SFileStatus, byte[]>(status, null));
    }
    return true;
  }

  @Override // FileSystem
  public synchronized FSDataOutputStream create(Path f,
      FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress)
          throws IOException {
    if(isBootStrapped)
      return super.create(f, permission, overwrite,
          bufferSize, replication, blockSize, progress);
    mkdirs(f.getParent());
    String src = f.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootStrapMap.get(src);
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
    bootStrapMap.put(src, new Pair<SFileStatus, byte[]>(status, null));
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
    if(isBootStrapped)
      return super.rename(src, dst);
    String s = src.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootStrapMap.remove(s);
    if(file == null)
      throw new FileNotFoundException("bootStrapMap does not contain " + src);
    String d = dst.toUri().getPath();
    file.getFirst().setPath(dst);
    bootStrapMap.put(d, file);
    return true;
  }

  @Override // FileSystem
  public synchronized boolean delete(Path f, boolean recursive)
      throws IOException {
    if(isBootStrapped)
      return super.delete(f, recursive);
    String src = f.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootStrapMap.remove(src);
    return file != null;
  }

  @Override // FileSystem
  public synchronized void setTimes(Path p, long mtime, long atime)
      throws IOException {
    String src = p.toUri().getPath();
    Pair<SFileStatus, byte[]> file = bootStrapMap.get(src);
    if(file == null)
      throw new FileNotFoundException("bootStrapMap does not contain " + src);
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
    bootStrapMap.put(src, file);
  }

  public synchronized void writeSuperBlock(Configuration conf)
          throws IOException {
    writeSuperBlock(conf, bootStrapMap);
    LOG.info("Super block contents: ");
    LOG.info(bootStrapMap.keySet().toString());
  }

  public synchronized void finalizeBootstrap(Configuration conf)
      throws IOException {
    isBootStrapped = true;
    LOG.info("Bootstrap is finalized.");
  }

  @SuppressWarnings("unchecked")
  public static
  Map<String, Pair<SFileStatus, byte[]>> readSuperBlock(Configuration conf)
      throws IOException {
    BSFSConfiguration bsConf = ((conf instanceof BSFSConfiguration) ?
        (BSFSConfiguration)conf : new BSFSConfiguration(conf));
    String bmAddress = bsConf.get(GRFA_BLOCK_MANAGER_ADDRESS_KEY);
    assert bmAddress != null : "bmAddress is null";
    Configuration hdfsConf = new Configuration(conf);
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
    FileSystem hdfs = FileSystem.get(new Path(bmAddress).toUri(), conf);
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
      synchronized(BSFileSystem.this) {
        Pair<SFileStatus, byte[]> file = bootStrapMap.get(src);
        if(file == null)
          throw new FileNotFoundException("bootStrapMap does not have " + src);
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
        bootStrapMap.put(src, file);
      }
    }
  }

  /**
   * Serializable FileStatus.
   * SHV!!! Should use protobuf to serialize the entire bootStrapMap.
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
}
