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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.FileField;
import org.apache.giraffa.GiraffaConstants.BlockAction;
import org.apache.giraffa.GiraffaPBHelper;
import org.apache.giraffa.INode;
import org.apache.giraffa.INodeDirectory;
import org.apache.giraffa.INodeFile;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.RowKeyBytes;
import org.apache.giraffa.RowKeyFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.Time;

import com.google.common.collect.Iterables;

/**
 * INodeManager maintains a mapping from Giraffa file INodes to HBase
 * table rows representing corresponding files.
 */
public class INodeManager implements Closeable {
  private static final Log LOG = LogFactory.getLog(INodeManager.class);
  private static final byte[] EMPTY = new byte[0];

  /** The Namespace table */
  private Table nsTable;

  public INodeManager(Table nsTable) {
    assert nsTable != null : "nsTable is null";
    this.nsTable = nsTable;
  }

  @Override
  public void close() {
    LOG.info("Closing INodeManager " + nsTable);
    try {
      if(nsTable != null) {
        nsTable.close();
        nsTable = null;
      }
    } catch (IOException e) {
      LOG.error("Cannot close table: ", e);
    }
  }

  public INode getParentINode(String path) throws IOException {
    Path parent = new Path(path).getParent();
    return parent == null ? null : getINode(parent.toString());
  }

  /**
   * Fetch an INode by source path String
   * @param path the source path String
   * @return INode for the specified path
   */
  public INode getINode(String path) throws IOException {
    return getINode(RowKeyFactory.newInstance(path));
  }

  /**
   * Fetch an INode, by RowKey.
   * @param key the RowKey
   * @return INode with the specified RowKey
   */
  public INode getINode(RowKey key) throws IOException {
    Result nodeInfo = getNSTable().get(new Get(key.getKey()));
    if(nodeInfo.isEmpty()) {
      LOG.debug("File does not exist: " + key.getPath());
      return null;
    }
    return newINode(key.getPath(), nodeInfo);
  }

  /**
   * Commit only the lease field of the given INode into HBase.
   */
  public void updateINodeLease(INodeFile node) throws IOException {
    long ts = Time.now();
    Put put = new Put(node.getRowKey().getKey(), ts);
    // lease update
    put.addColumn(FileField.getFileAttributes(), FileField.getLease(), ts,
        GiraffaPBHelper.hdfsLeaseToBytes(node.getLease()));

    getNSTable().put(put);
  }

  /**
   * Commit the fields of the given INode into HBase.
   */
  public void updateINode(INode node) throws IOException {
    updateINode(node, null, null);
  }

  /**
   * Commit the fields of the give INode into HBase. Additional stores a
   * BlockAction for processing by the BlockManagementAgent.
   */
  public void updateINode(INode node, BlockAction ba)
      throws IOException {
    updateINode(node, ba, null);
  }

  /**
   * Commit the fields of the give INode into HBase.
   * Additional stores a BlockAction for processing by the BlockManagementAgent.
   * If xAttrs is not null, it will add a list of XAttr to the node as well
   */
  public void updateINode(INode node, BlockAction ba, List<XAttr> xAttrs)
      throws IOException {
    long ts = Time.now();
    RowKey key = node.getRowKey();
    byte[] family = FileField.getFileAttributes();
    Put put = new Put(node.getRowKey().getKey(), ts);
    put.addColumn(family, FileField.getFileName(), ts,
            RowKeyBytes.toBytes(new Path(key.getPath()).getName()))
        .addColumn(family, FileField.getUserName(), ts,
            RowKeyBytes.toBytes(node.getOwner()))
        .addColumn(family, FileField.getGroupName(), ts,
            RowKeyBytes.toBytes(node.getGroup()))
        .addColumn(family, FileField.getPermissions(), ts,
            Bytes.toBytes(node.getPermission().toShort()))
        .addColumn(family, FileField.getMTime(), ts,
            Bytes.toBytes(node.getModificationTime()))
        .addColumn(family, FileField.getATime(), ts,
            Bytes.toBytes(node.getAccessTime()))
        .addColumn(family, FileField.getRenameState(), ts,
            GiraffaPBHelper.convert(node.getRenameState()).toByteArray());

    // symlink
    if(node.getSymlink() != null) {
      put.addColumn(family, FileField.getSymlink(), ts, node.getSymlink());
    }

    // file/directory specific columns
    if (node.isDir()) {
      INodeDirectory dir = INodeDirectory.valueOf(node);
      put.addColumn(family, FileField.getDirectory(), ts, EMPTY)
          .addColumn(family, FileField.getDsQuota(), ts,
              Bytes.toBytes(dir.getDsQuota()))
          .addColumn(family, FileField.getNsQuota(), ts,
              Bytes.toBytes(dir.getNsQuota()));
    } else {
      INodeFile file = INodeFile.valueOf(node);
      put.addColumn(family, FileField.getLength(), ts,
              Bytes.toBytes(file.getLen()))
          .addColumn(family, FileField.getReplication(), ts,
              Bytes.toBytes(file.getReplication()))
          .addColumn(family, FileField.getBlockSize(), ts,
              Bytes.toBytes(file.getBlockSize()))
          .addColumn(family, FileField.getBlock(), ts,
              GiraffaPBHelper.unlocatedBlocksToBytes(file.getBlocks()))
          .addColumn(family, FileField.getLocations(), ts,
              GiraffaPBHelper.blockLocationsToBytes(file.getLocations()))
          .addColumn(family, FileField.getFileState(), ts,
              Bytes.toBytes(file.getFileState().toString()))
          .addColumn(family, FileField.getLease(), ts,
              GiraffaPBHelper.hdfsLeaseToBytes(file.getLease()));
    }

    // block action
    if(ba != null) {
      put.addColumn(family, FileField.getAction(), ts, Bytes.toBytes(ba.toString()));
    }

    if (xAttrs != null) {
      for (XAttr xAttr : xAttrs) {
        String xAttrColumnName = XAttrHelper.getPrefixName(xAttr);
        put.addColumn(FileField.getFileExtendedAttributes(),
            Bytes.toBytes(xAttrColumnName), ts, xAttr.getValue());
      }
    }

    getNSTable().put(put);
  }

  /**
   * Apply the given function to each child of the specified directory.
   * @param root the directory whose children to scan
   * @param f the function to apply to each child INode
   */
  public void map(INode root, Function f) throws IOException {
    map(root, HdfsFileStatus.EMPTY_NAME, Integer.MAX_VALUE, f);
  }

  /**
   * Apply the given function to each child of the specified directory.
   * @param root the directory whose children to scan
   * @param startAfter the name to start scanning after encoded in java UTF8
   * @param limit the maximum number of nodes to scan
   * @param f the function to apply to each child INode
   */
  public void map(INode root, byte[] startAfter, int limit, Function f)
      throws IOException {
    RowKey key = root.getRowKey();
    ResultScanner rs = getListingScanner(key, startAfter);
    try {
      for(Result result : Iterables.limit(rs, limit)) {
        f.apply(newINodeByParent(key.getPath(), result));
      }
    } finally {
      rs.close();
    }
  }

  /**
   * Get a partial listing of the indicated directory.
   * @param dir the directory to list
   * @param startAfter the name to start listing after encoded in java UTF8
   * @param limit the maximum number of nodes to list
   * @return a list of INodes representing the children of the given directory
   */
  public List<INode> getListing(INode dir, byte[] startAfter, int limit)
      throws IOException {
    final List<INode> nodes = new ArrayList<INode>();
    map(dir, startAfter, limit, new Function() {
      @Override
      public void apply(INode input) throws IOException {
        nodes.add(input);
      }
    });
    return nodes;
  }

  /**
   * Recursively generates a list containing the given node and all
   * subdirectories. The nodes are found and stored in breadth-first order. For
   * each node, {@link INodeDirectory#isEmpty()} is guaranteed to return a
   * nonnull value.
   */
  public List<INodeDirectory> getDirectories(INodeDirectory root)
      throws IOException {
    List<INodeDirectory> directories = new ArrayList<>();
    directories.add(root);

    // start loop descending the tree (breadth first, then depth)
    for(int i = 0; i < directories.size(); i++) {
      // get next directory INode in the list and it's Scanner
      INodeDirectory dir = directories.get(i);
      dir.setEmpty(true);
      RowKey key = dir.getRowKey();
      ResultScanner rs = getListingScanner(key);
      try {
        for (Result result : rs) {
          dir.setEmpty(false);
          if (FileFieldDeserializer.getDirectory(result)) {
            INode child = newINodeByParent(key.getPath(), result);
            directories.add(INodeDirectory.valueOf(child));
          }
        }
      } finally {
        rs.close();
      }
    }

    return directories;
  }

  /**
   * Returns whether the given directory has any children.
   */
  public boolean isEmptyDirectory(INode dir) throws IOException {
    ResultScanner rs = getListingScanner(dir.getRowKey());
    try {
      return rs.next() == null;
    } finally {
      rs.close();
    }
  }

  /**
   * Deletes the given node's row from HBase.
   */
  public void delete(INode node) throws IOException {
    getNSTable().delete(new Delete(node.getRowKey().getKey()));
  }

  /**
   * Batch deletes the given nodes' rows from HBase
   */
  public void delete(List<? extends INode> nodes) throws IOException {
    List<Delete> deletes = new ArrayList<Delete>();
    for(INode node : nodes) {
      deletes.add(new Delete(node.getRowKey().getKey()));
    }
    getNSTable().delete(deletes);
  }

  /**
   * Gets the blocks and locations for the given INode from HBase and updates
   * the INode with the obtained information.
   */
  public void getBlocksAndLocations(INodeFile file) throws IOException {
    Result result = getNSTable().get(new Get(file.getRowKey().getKey()));
    file.setBlocks(FileFieldDeserializer.getBlocks(result));
    file.setLocations(FileFieldDeserializer.getLocations(result));
  }

  public void setXAttr(String path, XAttr xAttr) throws IOException {
    long ts = Time.now();
    RowKey rowKey = RowKeyFactory.newInstance(path);
    Put put = new Put(rowKey.getKey(), ts);
    String realColumnName = XAttrHelper.getPrefixName(xAttr);
    put.addColumn(FileField.getFileExtendedAttributes(),
            Bytes.toBytes(realColumnName), ts, xAttr.getValue());
    getNSTable().put(put);
  }

  public List<XAttr> getXAttrs(String path) throws IOException {
    RowKey rowKey = RowKeyFactory.newInstance(path);
    Result result = getNSTable().get(new Get(rowKey.getKey()));
    return FileFieldDeserializer.getXAttrs(result);
  }

  public void removeXAttr(String path, XAttr xAttr) throws IOException {
    RowKey rowKey = RowKeyFactory.newInstance(path);
    Delete delete = new Delete(rowKey.getKey());
    String realColumnName = XAttrHelper.getPrefixName(xAttr);
    delete.addColumns(FileField.getFileExtendedAttributes(),
                      Bytes.toBytes(realColumnName));
    getNSTable().delete(delete);
  }

  private Table getNSTable() {
    assert nsTable != null : "No Table is set.";
    return nsTable;
  }

  private INode newINodeByParent(String parent, Result res) throws IOException {
    String fileName = FileFieldDeserializer.getFileName(res);
    return newINode(new Path(parent, fileName).toString(), res);
  }

  private INode newINode(String src, Result result) throws IOException {
    RowKey key = RowKeyFactory.newInstance(src, result.getRow());
    if (FileFieldDeserializer.getDirectory(result)) {
      return new INodeDirectory(key,
          FileFieldDeserializer.getMTime(result),
          FileFieldDeserializer.getATime(result),
          FileFieldDeserializer.getUserName(result),
          FileFieldDeserializer.getGroupName(result),
          FileFieldDeserializer.getPermissions(result),
          FileFieldDeserializer.getSymlink(result),
          FileFieldDeserializer.getRenameState(result),
          FileFieldDeserializer.getDsQuota(result),
          FileFieldDeserializer.getNsQuota(result));
    } else {
      return new INodeFile(key,
          FileFieldDeserializer.getMTime(result),
          FileFieldDeserializer.getATime(result),
          FileFieldDeserializer.getUserName(result),
          FileFieldDeserializer.getGroupName(result),
          FileFieldDeserializer.getPermissions(result),
          FileFieldDeserializer.getSymlink(result),
          FileFieldDeserializer.getRenameState(result),
          FileFieldDeserializer.getLength(result),
          FileFieldDeserializer.getReplication(result),
          FileFieldDeserializer.getBlockSize(result),
          FileFieldDeserializer.getFileState(result),
          FileFieldDeserializer.getLease(result),
          FileFieldDeserializer.getBlocks(result),
          FileFieldDeserializer.getLocations(result));
    }
  }

  private ResultScanner getListingScanner(RowKey key)
      throws IOException {
    return getListingScanner(key, HdfsFileStatus.EMPTY_NAME);
  }

  private ResultScanner getListingScanner(RowKey key, byte[] startAfter)
      throws IOException {
    byte[] start = key.getStartListingKey(startAfter);
    byte[] stop = key.getStopListingKey();
    return getNSTable().getScanner(new Scan(start, stop));
  }

  public interface Function {
    void apply(INode input) throws IOException;
  }
}
