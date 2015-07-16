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

import com.google.common.collect.Lists;

import org.apache.giraffa.FSPermissionChecker;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.security.AccessControlException;

import java.util.List;

/**
 * Copied and refined from
 * ${@link org.apache.hadoop.hdfs.server.namenode.XAttrPermissionFilter}
 * Since that one is private
 * <p>
 * The logic is the same as its original, only coding style is refined
 */
public class XAttrPermissionFilter {
  static void checkPermissionForApi(FSPermissionChecker pc, XAttr xAttr)
      throws AccessControlException {
    assert (pc != null && xAttr != null) : "Argument is null";
    if (xAttr.getNameSpace() != XAttr.NameSpace.USER &&
       (xAttr.getNameSpace() != XAttr.NameSpace.TRUSTED || !pc.isSuperUser())) {
      throw new AccessControlException("User doesn\'t have permission"
         + " for xAttr: " + XAttrHelper.getPrefixName(xAttr));
    }
  }

  static void checkPermissionForApi(FSPermissionChecker pc, List<XAttr> xAttrs)
      throws AccessControlException {
    assert (xAttrs != null) : "Argument is null";
    for (XAttr xAttr : xAttrs) {
      checkPermissionForApi(pc, xAttr);
    }
  }

  static List<XAttr> filterXAttrsForApi(FSPermissionChecker pc,
                                        List<XAttr> xAttrs) {
    assert (xAttrs != null) : "Argument is null";
    List<XAttr> filteredXAttrs =
        Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      if (xAttr.getNameSpace() == XAttr.NameSpace.USER ||
        (xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED && pc.isSuperUser())) {
        filteredXAttrs.add(xAttr);
      }
    }
    return filteredXAttrs;
  }
}
