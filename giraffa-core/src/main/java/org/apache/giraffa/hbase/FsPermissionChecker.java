package org.apache.giraffa.hbase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.INode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * Class that helps in checking file system permission.
 * The state of this class need not be synchronized as it has data structures that
 * are read-only.
 * Some of the helper methods are gaurded by {@link FSNamesystem#readLock()}.
 */
public class FsPermissionChecker {
  static final Log LOG = LogFactory.getLog(UserGroupInformation.class);

  public static final String FS_OWNER = "hdfs";
  public static final String FS_SUPERGROUP = "supergroup";

  private final UserGroupInformation ugi;
  private final String user;
  private final String primaryGroup;
  /**
   * A set with group namess. Not synchronized since it is unmodifiable
   */
  private final Set<String> groups;
  private final boolean isSuper;
  private final INodeResolver resolver;

  public interface INodeResolver {
    INode find(String path) throws IOException;

    List<INode> findINodesInPath(String path) throws IOException;

    List<INode> findChildren(String path) throws IOException;
  }

  /**
   * TODO: make fs owner and supergroup configurable
   */
  public FsPermissionChecker(UserGroupInformation callerUgi,
      INodeResolver resolver) {
    this.ugi = callerUgi;
    String[] groupNames = ugi.getGroupNames();
    this.primaryGroup = groupNames.length > 0 ? groupNames[0] : null;
    HashSet<String> s = new HashSet<String>(Arrays.asList(groupNames));
    this.groups = Collections.unmodifiableSet(s);
    this.user = ugi.getShortUserName();
    this.isSuper = user.equals(FS_OWNER) || groups.contains(FS_SUPERGROUP);
    this.resolver = resolver;
  }

  /**
   * @return a string for throwing {@link AccessControlException}
   */
  private String toAccessControlString(
      INode inode,
      FsAction access, FsPermission mode) {
    return toAccessControlString(inode, access, mode, null);
  }

  /**
   * @return a string for throwing {@link AccessControlException}
   */
  private String toAccessControlString(
      INode inode, FsAction access, FsPermission mode,
      List<AclEntry> featureEntries) {
    StringBuilder sb = new StringBuilder("Permission denied: ")
        .append("user=").append(user).append(", ")
        .append("access=").append(access).append(", ")
        .append("inode=\"").append(inode.getRowKey()).append("\":")
        .append(inode.getOwner()).append(':')
        .append(inode.getGroup()).append(':')
        .append(inode.isDir() ? 'd' : '-')
        .append(mode);
    if (featureEntries != null) {
      sb.append(':').append(StringUtils.join(",", featureEntries));
    }
    return sb.toString();
  }

  /**
   * Check if the callers group contains the required values.
   *
   * @param group group to check
   */
  public boolean containsGroup(String group) {
    return groups.contains(group);
  }

  public String getUser() {
    return user;
  }

  public String getPrimaryGroup() {
    return primaryGroup;
  }

  public boolean isSuperUser() {
    return isSuper;
  }

  public void checkSuperuserPrivilege()
      throws AccessControlException {
    if (!isSuper) {
      throw new AccessControlException("Access denied for user "
          + user + ". Superuser privilege is required");
    }
  }

  public String getSuperUser() {
    return FS_OWNER;
  }

  public String getSuperGroup() {
    return FS_OWNER;
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkOwner(INode inode) throws AccessControlException {
    if (inode != null && user.equals(inode.getOwner())) {
      return;
    }
    throw new AccessControlException("Permission denied");
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  public void checkTraverse(INode[] inodes, int last)
      throws AccessControlException {
    for (int j = 0; j <= last; j++) {
      check(inodes[j], FsAction.EXECUTE);
    }
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  public void checkSubAccess(INode inode, FsAction access,
      boolean ignoreEmptyDir)
      throws IOException {
    if (inode == null || !inode.isDir()) {
      return;
    }

    Stack<INode> directories = new Stack<INode>();
    for (directories.push(inode); !directories.isEmpty(); ) {
      INode d = directories.pop();
      List<INode> cList =
          resolver.findChildren(d.getRowKey().getPath());
      if (!(cList.isEmpty() && ignoreEmptyDir)) {
        check(d, access);
      }

      for (INode child : cList) {
        if (child.isDir()) {
          directories.push(child);
        }
      }
    }
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  public void check(INode[] inodes, int i, FsAction access
  ) throws AccessControlException {
    check(i >= 0 ? inodes[i] : null, access);
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  public void check(INode inode, FsAction access
  ) throws AccessControlException {
    if (inode == null) {
      return;
    }
    FsPermission mode = inode.getPermission();
    checkFsPermission(inode, access, mode);
  }

  public void checkFsPermission(INode inode, FsAction access,
      FsPermission mode) throws AccessControlException {
    if (user.equals(inode.getOwner())) { //user class
      if (mode.getUserAction().implies(access)) {
        return;
      }
    } else if (groups.contains(inode.getGroup())) { //group class
      if (mode.getGroupAction().implies(access)) {
        return;
      }
    } else { //other class
      if (mode.getOtherAction().implies(access)) {
        return;
      }
    }
    throw new AccessControlException(
        toAccessControlString(inode, access, mode));
  }

  /**
   * Checks requested access against an Access Control List.  This method relies
   * on finding the ACL data in the relevant portions of {@link FsPermission} and
   * {@link AclFeature} as implemented in the logic of {@link AclStorage}.  This
   * method also relies on receiving the ACL entries in sorted order.  This is
   * assumed to be true, because the ACL modification methods in
   * {@link AclTransformation} sort the resulting entries.
   * More specifically, this method depends on these invariants in an ACL:
   * - The list must be sorted.
   * - Each entry in the list must be unique by scope + type + name.
   * - There is exactly one each of the unnamed user/group/other entries.
   * - The mask entry must not have a name.
   * - The other entry must not have a name.
   * - Default entries may be present, but they are ignored during enforcement.
   *
   * @param inode          INode accessed inode
   * @param snapshotId     int snapshot ID
   * @param access         FsAction requested permission
   * @param mode           FsPermission mode from inode
   * @param featureEntries List<AclEntry> ACL entries from AclFeature of inode
   * @throws AccessControlException if the ACL denies permission
   */
  public void checkAccessAcl(INode inode, FsAction access,
      FsPermission mode, List<AclEntry> featureEntries)
      throws AccessControlException {
    boolean foundMatch = false;

    // Use owner entry from permission bits if user is owner.
    if (user.equals(inode.getOwner())) {
      if (mode.getUserAction().implies(access)) {
        return;
      }
      foundMatch = true;
    }

    // Check named user and group entries if user was not denied by owner entry.
    if (!foundMatch) {
      for (AclEntry entry : featureEntries) {
        if (entry.getScope() == AclEntryScope.DEFAULT) {
          break;
        }
        AclEntryType type = entry.getType();
        String name = entry.getName();
        if (type == AclEntryType.USER) {
          // Use named user entry with mask from permission bits applied if user
          // matches name.
          if (user.equals(name)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
          }
        } else if (type == AclEntryType.GROUP) {
          // Use group entry (unnamed or named) with mask from permission bits
          // applied if user is a member and entry grants access.  If user is a
          // member of multiple groups that have entries that grant access, then
          // it doesn't matter which is chosen, so exit early after first match.
          String group = name == null ? inode.getGroup() : name;
          if (groups.contains(group)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
          }
        }
      }
    }

    // Use other entry if user was not denied by an earlier match.
    if (!foundMatch && mode.getOtherAction().implies(access)) {
      return;
    }

    throw new AccessControlException(
        toAccessControlString(inode, access, mode, featureEntries));
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  public void checkStickyBit(INode parent, INode inode
  ) throws AccessControlException {
    if (!parent.getPermission().getStickyBit()) {
      return;
    }

    // If this user is the directory owner, return
    if (parent.getOwner().equals(user)) {
      return;
    }

    // if this user is the file owner, return
    if (inode.getOwner().equals(user)) {
      return;
    }

    throw new AccessControlException(
        "Permission denied by sticky bit setting:" +
            " user=" + user + ", inode=" + inode);
  }

  public void checkOwner(String path)
      throws IOException {
    checkPermission(path, true, null, null, null, null);
  }

  public void checkPathAccess(
      String path, FsAction access) throws IOException {
    checkPermission(path, false, null, null, access, null);
  }

  public void checkParentAccess(
      String path, FsAction access) throws IOException {
    checkPermission(path, false, null, access, null, null);
  }

  public void checkAncestorAccess(
      String path, FsAction access) throws IOException {
    checkPermission(path, false, access, null, null, null);
  }

  public void checkTraverse(String path)
      throws IOException {
    checkPermission(path, false, null, null, null, null);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  public void checkPermission(
      String path, boolean doCheckOwner, FsAction ancestorAccess,
      FsAction parentAccess, FsAction access, FsAction subAccess)
      throws IOException {
    checkPermission(path, doCheckOwner, ancestorAccess,
        parentAccess, access, subAccess, false, true);
  }

  /**
   * Check whether current user have permissions to access the path.
   * Traverse is always checked.
   * Parent path means the parent directory for the path.
   * Ancestor path means the last (the closest) existing ancestor directory
   * of the path.
   * Note that if the parent path exists,
   * then the parent path and the ancestor path are the same.
   * For example, suppose the path is "/foo/bar/baz".
   * No matter baz is a file or a directory,
   * the parent path is "/foo/bar".
   * If bar exists, then the ancestor path is also "/foo/bar".
   * If bar does not exist and foo exists,
   * then the ancestor path is "/foo".
   * Further, if both foo and bar do not exist,
   * then the ancestor path is "/".
   *
   * @param doCheckOwner   Require user to be the owner of the path?
   * @param ancestorAccess The access required by the ancestor of the path.
   * @param parentAccess   The access required by the parent of the path.
   * @param access         The access required by the path.
   * @param subAccess      If path is a directory,
   *                       it is the access required of the path and all the sub-directories.
   *                       If path is not a directory, there is no effect.
   * @param ignoreEmptyDir Ignore permission checking for empty directory?
   * @param resolveLink    whether to resolve the final path component if it is
   *                       a symlink
   * @throws AccessControlException
   * @throws UnresolvedLinkException Guarded by {@link FSNamesystem#readLock()}
   *                                 Caller of this method must hold that lock.
   */
  void checkPermission(String path, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess, boolean ignoreEmptyDir, boolean resolveLink)
      throws IOException {
    if (!isSuperUser()) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("ACCESS CHECK: " + this
          + ", doCheckOwner=" + doCheckOwner
          + ", ancestorAccess=" + ancestorAccess
          + ", parentAccess=" + parentAccess
          + ", access=" + access
          + ", subAccess=" + subAccess
          + ", ignoreEmptyDir=" + ignoreEmptyDir
          + ", resolveLink=" + resolveLink);
    }
    // check if (parentAccess != null) && file exists, then check sb
    // If resolveLink, the check is performed on the link target.
    final List<INode> inodesInPath = resolver.findINodesInPath(path);
    final INode[] inodes = inodesInPath
        .toArray(new INode[inodesInPath.size()]);
    int ancestorIndex = inodes.length - 2;
    while (ancestorIndex >= 0 && inodes[ancestorIndex] == null) {
      ancestorIndex--;
    }
    checkTraverse(inodes, ancestorIndex);

    final INode last = inodes[inodes.length - 1];
    if (parentAccess != null && parentAccess.implies(FsAction.WRITE)
        && inodes.length > 1 && last != null) {
      checkStickyBit(inodes[inodes.length - 2], last);
    }
    if (ancestorAccess != null && inodes.length > 1) {
      check(inodes, ancestorIndex, ancestorAccess);
    }
    if (parentAccess != null && inodes.length > 1) {
      check(inodes, inodes.length - 2, parentAccess);
    }
    if (access != null) {
      check(last, access);
    }
    if (subAccess != null) {
      checkSubAccess(last, subAccess, ignoreEmptyDir);
    }
    if (doCheckOwner) {
      checkOwner(last);
    }
  }

  public static INodeResolver getResolver(final INodeManager nodeManager) {
    return new INodeResolver() {
      @Override
      public INode find(String path) throws IOException {
        return nodeManager.getINode(path);
      }

      @Override
      public List<INode> findINodesInPath(String pathStr)
          throws IOException {

        Path path = new Path(pathStr);
        INode node = find(path.toString()); // null can be there
        List<INode> nodes = new ArrayList<INode>();
        Path parent = path;
        while ((parent = parent.getParent()) != null) {
          nodes.add(find(parent.toUri().getPath()));
        }
        nodes.add(node); // add last component of the path
        return nodes;
      }

      @Override
      public List<INode> findChildren(String path)
          throws IOException {
        INode node = find(path);
        if (node == null) {
          throw new NoSuchFileException(path);
        }

        return nodeManager.getDirectories(node);
      }
    };
  }

}