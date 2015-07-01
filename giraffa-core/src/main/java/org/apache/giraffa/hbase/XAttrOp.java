package org.apache.giraffa.hbase;

import com.google.common.collect.Lists;
import org.apache.giraffa.INode;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * TODO. This is temp version so the java doc will write later
 * Handle all detail operations logic
 * It can be treated as a simplified version of
 * org.apache.hadoop.hdfs.server.namenode.FSDirXAttrOp and
 * org.apache.hadoop.hdfs.server.namenode.XAttrStorage
 *
 * Candidate of permission to check
 * 1. Check if XAttribute feature is enable
 * 2. Check for XAttribute permission (system/user)
 * 3. Check for path permission (do we have write permission on specific path)
 * 4. Check for file system permission (does anyone blocking the file system?)
 * 5. validate if flag is valid
 * 6. Check attribute size (max len of name, the limited attribute to a file )
 */
public class XAttrOp {
  private INodeManager nodeManager;

  public XAttrOp(INodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
          throws IOException {
    if (src == null && xAttr == null && flag == null) {
      throw new IllegalArgumentException("Argument is null");
    }

    checkIfFileExisted(src);

    // TODO. Complete all permission checking
    // TODO. Think about what kind of permissions to be checked exactly
    // do the permission checking.

    // check if we can overwrite
    boolean isAttrExisted = false;
    List<XAttr> oldXAttrList = listXAttrs(src);
    if (oldXAttrList != null) {
      for (XAttr oldAttr : oldXAttrList) {
        if (oldAttr != null && xAttr.getName().equals(oldAttr.getName())) {
          XAttrSetFlag.validate(xAttr.getName(), true, flag);
          isAttrExisted = true;
          break;
        }
      }
    }
    if (!isAttrExisted) { // in this case, need checked if CREATE is set
      XAttrSetFlag.validate(xAttr.getName(), false, flag);
    }

    nodeManager.setXAttr(src, xAttr); // TODO, merge to updateINode
  }

  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
          throws IOException {
    if (src == null && xAttrs == null) {
      throw new IllegalArgumentException("Argument is null");
    }
    checkIfFileExisted(src);
    final boolean isGetAll =
            (xAttrs == null || xAttrs.isEmpty()) ? true : false;

    // TODO some permission checking here
    //  1. RawPath checking
    if (!isGetAll) {
      // TODO check permission of input attr list : xAttr
    }
    // TODO: check if we have access permission to path src

    List<XAttr> oldXAttrList = listXAttrs(src);
    // TODO, filter oldXAttrList (filter out those with permission problems)

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
        if (neededXAttr.getNameSpace() == oldXAttr.getNameSpace() &&
                neededXAttr.getName().equals(oldXAttr.getName())) {
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
    if (src == null) {
      throw new IllegalArgumentException("Argument is null");
    }

    checkIfFileExisted(src);
    // TODO. more permission checking ?
    return nodeManager.listXAttrs(src);
    // TODO. permission checking ? Filter result list
  }

  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    if (src == null && xAttr == null) {
      throw new IllegalArgumentException("Argument is null");
    }

    checkIfFileExisted(src);
    // TODO permission checking

    // check if the attributes existed or not
    List<XAttr> targetXAttrList = Lists.newArrayListWithCapacity(1);
    targetXAttrList.add(xAttr);
    try {
      getXAttrs(src, targetXAttrList);
    } catch (IOException e) {
      throw new IOException(
        "No matching attributes found for remove operation");
    }

    nodeManager.removeXAttr(src, xAttr); // TODO, merge to updateINode
  }

  private INode checkIfFileExisted(String src) throws IOException {
    INode node = nodeManager.getINode(src);
    if (node == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    return node;
  }
}
