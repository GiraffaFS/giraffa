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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Class that helps in checking file system permission.
 * The state of this class need not be synchronized as it has data structures that
 * are read-only.
 */
public class FSPermissionChecker {

  private final String user;
  private final Set<String> groups;
  private final boolean isSuper;

  public FSPermissionChecker(String fsOwner, String supergroup,
                             UserGroupInformation callerUgi) {
    user = callerUgi.getShortUserName();
    HashSet<String> s =
        new HashSet<String>(Arrays.asList(callerUgi.getGroupNames()));
    groups = Collections.unmodifiableSet(s);
    isSuper = user.equals(fsOwner) || groups.contains(supergroup);
  }

  public boolean containsGroup(String group) {
    return groups.contains(group);
  }

  public String getUser() {
    return user;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public boolean isSuperUser() {
    return isSuper;
  }

  /**
   * Verify if the caller has the required permission. This will result into
   * an exception if the caller is not allowed to access the resource.
   */
  public void checkSuperuserPrivilege()
      throws AccessControlException {
    if (!isSuperUser()) {
      throw new AccessControlException("Access denied for user "
          + getUser() + ". Superuser privilege is required");
    }
  }

  public void checkOwner(INode inode)
      throws AccessControlException {
    if (isSuperUser() || getUser().equals(inode.getOwner())) {
      return;
    }
    throw new AccessControlException(
            "Permission denied. user="
            + getUser() + " is not the owner of inode=" + inode);
  }

  public void check(INode inode, FsAction access)
      throws AccessControlException {
    if (inode == null || isSuperUser()) {
      return;
    }
    final FsPermission mode = inode.getPermission();
    if (getUser().equals(inode.getOwner())) { //user class
      if (mode.getUserAction().implies(access)) { return; }
    }
    else if (getGroups().contains(inode.getGroup())) { //group class
      if (mode.getGroupAction().implies(access)) { return; }
    }
    else { //other class
      if (mode.getOtherAction().implies(access)) { return; }
    }
    throw new AccessControlException(
        toAccessControlString(inode, access, mode));
  }

  public void checkStickyBit(INode parent, INode inode)
      throws AccessControlException {
    if(parent == null || inode == null || isSuperUser()) {
      return;
    }

    if(!parent.getPermission().getStickyBit()) {
      return;
    }

    // If this user is the directory owner, return
    if(parent.getOwner().equals(user)) {
      return;
    }

    // if this user is the file owner, return
    if(inode.getOwner().equals(user)) {
      return;
    }

    throw new AccessControlException("Permission denied by sticky bit setting:"
        + " user=" + user + ", inode=" + inode);
  }

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INode inode, FsAction access,
                                       FsPermission mode) {
    return new StringBuilder("Permission denied: ")
        .append("user=").append(getUser()).append(", ")
        .append("access=").append(access).append(", ")
        .append("inode=\"").append(inode.getPath()).append("\":")
        .append(inode.getOwner()).append(':')
        .append(inode.getGroup()).append(':')
        .append(inode.isDir() ? 'd' : '-')
        .append(mode).toString();
  }
}
