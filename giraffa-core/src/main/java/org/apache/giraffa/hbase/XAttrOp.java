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

import static org.apache.giraffa.hbase.XAttrPermissionFilter.checkPermissionForApi;
import static org.apache.giraffa.hbase.XAttrPermissionFilter.filterXAttrsForApi;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;

import com.google.common.collect.Lists;

import org.apache.giraffa.FSPermissionChecker;
import org.apache.giraffa.INode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hbase.ipc.HBaseRpcUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

/**
 * It's an intermediate class between {@link NamespaceProcessor} and
 * {@link INodeManager} to handle XAttr related operations. In
 * NamespaceProcessor, it has basic permission checking to see
 * if XAttr configuration flag is set and if a xAttr is valid. In
 * INodeManager, it communicates with Hbase to put/get XAttr info.
 * In this method, it covers all the rest parts. For instance,
 * it has additional permission checking on input arguments from
 * NamespaceProcessor. After processing, it generates query parameters
 * to INodeManager for a Hbase put/get command. Finally, it
 * filters returned lists under some circumstances and then returns
 * the result to NamespaceProcessor.
 * <p>
 * The idea comes from Hadoop source code. It's mainly based on
 * Hadoop 2.5 since that's the compatible version when this class
 * is developed. Please note there are various style/design changes
 * between Hadoop 2.5 and its later versions in XAttr parts. Basically,
 * the followings are a comparison of XAttr handling between
 * Giraffa and Hadoop 2.5.
 * <p>
 * There's a list of classes in Hadoop 2.5 and their counterparts in Giraffa
 * {@link org.apache.hadoop.hdfs.server.namenode.NNConf}
 *   Half functions are in NamespaceProcessor and the other half
 *   are in this class.
 * {@link org.apache.hadoop.hdfs.server.namenode.XAttrStorage}
 *   All its functions are simplified in INodeManager.
 * {@link org.apache.hadoop.hdfs.server.namenode.FSNamesystem}
 *   All XAttr related functions are in this class.
 * {@link org.apache.hadoop.hdfs.server.namenode.FSDirectory}
 *   All XAttr related functions are in this class.
 * <p>
 * There are bunch of permission checking in Hadoop code.
 * Some of them are also in Giraffa
 *   1. Check if XAttribute feature is enable
 *      (Checked in NamespaceProcessor)
 *   2. Check for XAttribute permission (System/Security/Trusted/User)
 *   3. Check for path permission (do we have write permission on specific path)
 *   4. Validate if flag is valid
 *   5. Check attribute size (max len of name and value)
 *      (Checked in NamespaceProcessor)
 *   6. Check if exceed limit size of attr for given node
 * While some are not
 *   1. Check for file system permission (does anyone blocking the file system?)
 *      read lock/write lock.
 *   2. Check if save mode (safe mode is not supported now)
 */
public class XAttrOp {
  private INodeManager nodeManager;
  private int inodeXAttrsLimit;
  private String fsOwnerShortUserName;
  private String supergroup;
  private boolean isPermissionEnabled;

  public XAttrOp(INodeManager nodeManager, Configuration conf)
      throws IOException{
    this.nodeManager = nodeManager;
    this.inodeXAttrsLimit = conf.getInt(DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
                            DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);
    assert inodeXAttrsLimit >= 0 :
        "Cannot set a negative limit on the number of xAttrs per inode (" +
        DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY + ").";
    UserGroupInformation fsOwner = UserGroupInformation.getCurrentUser();
    fsOwnerShortUserName = fsOwner.getShortUserName();
    supergroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
                          DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
                                          DFS_PERMISSIONS_ENABLED_DEFAULT);
  }

  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
          throws IOException {
    assert (src != null && xAttr != null && flag != null) : "Argument is null";
    checkIfFileExisted(src);
    FSPermissionChecker pc = getFsPermissionChecker();
    checkXAttrChangeAccess(src, xAttr, pc);
    checkPermissionForApi(pc, xAttr);

    // check if we can overwrite/ exceed attr numbers limit of a file
    boolean isAttrExisted = false;
    int userVisibleXAttrsNum = 0;
    List<XAttr> oldXAttrList = listXAttrs(src);
    if (oldXAttrList != null) {
      for (XAttr oldAttr : oldXAttrList) {
        if (xAttr.equalsIgnoreValue(oldAttr)) {
          XAttrSetFlag.validate(xAttr.getName(), true, flag);
          isAttrExisted = true;
        }
        if (isUserVisible(oldAttr)){
          ++userVisibleXAttrsNum;
        }
      }
    }
    if (!isAttrExisted) { // in this case, need checked if CREATE is set
      XAttrSetFlag.validate(xAttr.getName(), false, flag);
      if (isUserVisible(xAttr)) {
        ++userVisibleXAttrsNum;
      }
    }
    if(userVisibleXAttrsNum > inodeXAttrsLimit) {
      throw new IOException("Cannot add additional XAttr to inode,"
                            + " would exceed limit of " + inodeXAttrsLimit);
    }
    nodeManager.setXAttr(src, xAttr);
  }

  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
          throws IOException {
    assert (src != null) : "Argument is null";
    checkIfFileExisted(src);
    final boolean isGetAll = (xAttrs == null || xAttrs.isEmpty());
    FSPermissionChecker pc = getFsPermissionChecker();
    if (!isGetAll && isPermissionEnabled) {
      checkPermissionForApi(pc, xAttrs);
    }
    checkPathAccess(pc, src, FsAction.READ);

    List<XAttr> oldXAttrList = nodeManager.getXAttrs(src);
    oldXAttrList = filterXAttrsForApi(pc, oldXAttrList);
    if (isGetAll) {
      return oldXAttrList;
    }

    /* now we need return elements from oldXAttrList which is in xAttrs */
    if (oldXAttrList == null || oldXAttrList.isEmpty()) {
      throw new IOException(
              "At least one of the attributes provided was not found.");
    }
    List<XAttr> resXAttrList = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr neededXAttr : xAttrs) {
      boolean foundIt = false;
      for (XAttr oldXAttr : oldXAttrList) {
        if (neededXAttr.equalsIgnoreValue(oldXAttr)) {
          resXAttrList.add(oldXAttr);
          foundIt = true;
          break;
        }
      }
      if (!foundIt) {
        throw new IOException(
                "At least one of the attributes provided was not found.");
      }
    }
    return resXAttrList;
  }

  public List<XAttr> listXAttrs(String src) throws IOException {
    assert (src != null) : "Argument is null";
    FSPermissionChecker pc = getFsPermissionChecker();
    checkIfFileExisted(src);
    if (isPermissionEnabled) {
      checkParentAccess(pc, src, FsAction.EXECUTE);
    }
    return filterXAttrsForApi(pc, nodeManager.getXAttrs(src));
  }

  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    assert (src != null && xAttr != null) : "Argument is null";
    checkIfFileExisted(src);
    FSPermissionChecker pc = getFsPermissionChecker();
    if (isPermissionEnabled) {
      checkPermissionForApi(pc, xAttr);
    }
    checkXAttrChangeAccess(src, xAttr, pc);

    // check if the attributes existed or not
    List<XAttr> targetXAttrList = Lists.newArrayListWithCapacity(1);
    targetXAttrList.add(xAttr);
    try {
      getXAttrs(src, targetXAttrList);
    } catch (IOException e) {
      throw new IOException(
        "No matching attributes found for remove operation");
    }
    nodeManager.removeXAttr(src, xAttr);
  }

  private INode checkIfFileExisted(String src) throws IOException {
    INode node = nodeManager.getINode(src);
    if (node == null) {
      throw new FileNotFoundException("cannot find " + src);
    }
    return node;
  }

  private boolean isUserVisible(XAttr xAttr) {
    return xAttr.getNameSpace() == XAttr.NameSpace.USER ||
           xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED;
  }

  private FSPermissionChecker getFsPermissionChecker() throws IOException {
    UserGroupInformation ugi = HBaseRpcUtil.getRemoteUser();
    return new FSPermissionChecker(fsOwnerShortUserName, supergroup, ugi);
  }

  private void checkXAttrChangeAccess(String src, XAttr xAttr,
    FSPermissionChecker pc) throws IOException {
    if(isPermissionEnabled && xAttr.getNameSpace() == XAttr.NameSpace.USER) {
      INode node = nodeManager.getINode(src);
      if(node.isDir() && node.getPermission().getStickyBit()) {
        if(!pc.isSuperUser()) {
          pc.checkOwner(node);
        }
      } else {
        checkPathAccess(pc, src, FsAction.WRITE);
        // Please note in legacy code it does not check X permission on
        // parent because the permission checking method will call
        // checkTraverse() automatically to check whole path.
        // However, in Giraffa, we need check parent manually since there's
        // no checkTraverse()
        // In addition, in Giraffa, we only check parent and don't
        // care grandparent
        checkParentAccess(pc, src, FsAction.EXECUTE);
      }
    }
  }

  private void checkPathAccess(FSPermissionChecker pc, String src,
                               FsAction access) throws IOException{
    INode node = nodeManager.getINode(src);
    pc.check(node, access);
  }

  private void checkParentAccess(FSPermissionChecker pc, String src,
                                 FsAction access) throws IOException{
    INode node = nodeManager.getParentINode(src);
    pc.check(node, access);
  }
}
