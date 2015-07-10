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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.giraffa.FSPermissionChecker;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.security.AccessControlException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * copy from
 * ${@link org.apache.hadoop.hdfs.server.namenode.XAttrPermissionFilter}
 * since that one is private
 */
public class XAttrPermissionFilter {
  public XAttrPermissionFilter() {
  }

  static void checkPermissionForApi( FSPermissionChecker pc, XAttr xAttr)
      throws AccessControlException {
    if(xAttr.getNameSpace() != XAttr.NameSpace.USER &&
       (xAttr.getNameSpace() != XAttr.NameSpace.TRUSTED || !pc.isSuperUser())) {
      throw new AccessControlException("User doesn\'t have permission"
         + " for xattr: " + XAttrHelper.getPrefixName(xAttr));
    }
  }

  static void checkPermissionForApi(FSPermissionChecker pc, List<XAttr> xAttrs)
      throws AccessControlException {
    Preconditions.checkArgument(xAttrs != null);
    if(!xAttrs.isEmpty()) {
      Iterator i$ = xAttrs.iterator();

      while(i$.hasNext()) {
        XAttr xAttr = (XAttr)i$.next();
        checkPermissionForApi(pc, xAttr);
      }
    }
  }

  static List<XAttr> filterXAttrsForApi(FSPermissionChecker pc,
                                        List<XAttr> xAttrs) {
    assert xAttrs != null : "xAttrs can not be null";

    if(xAttrs != null && !xAttrs.isEmpty()) {
      ArrayList filteredXAttrs = Lists.newArrayListWithCapacity(xAttrs.size());
      Iterator i$ = xAttrs.iterator();

      while(i$.hasNext()) {
        XAttr xAttr = (XAttr)i$.next();
        if(xAttr.getNameSpace() == XAttr.NameSpace.USER) {
          filteredXAttrs.add(xAttr);
        } else if(xAttr.getNameSpace() ==
                  XAttr.NameSpace.TRUSTED && pc.isSuperUser()) {
          filteredXAttrs.add(xAttr);
        }
      }

      return filteredXAttrs;
    } else {
      return xAttrs;
    }
  }
}
