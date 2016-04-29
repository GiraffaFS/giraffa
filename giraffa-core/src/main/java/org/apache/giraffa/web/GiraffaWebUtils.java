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
package org.apache.giraffa.web;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.giraffa.GiraffaConfiguration;
import org.apache.giraffa.GiraffaFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class GiraffaWebUtils {

  public static GiraffaFileSystem getGiraffaFileSystem(
      ServletContext servletContext) throws IOException {
    GiraffaFileSystem grfs = null;
    if (servletContext.getAttribute("grfa") == null) {
      grfs = (GiraffaFileSystem) FileSystem.get(new GiraffaConfiguration());
      servletContext.setAttribute("grfa", grfs);
    } else {
      grfs = (GiraffaFileSystem) servletContext.getAttribute("grfa");
    }
    return grfs;
  }

  @SuppressWarnings("deprecation")
  public static Path extractPath(HttpServletRequest request) {
    String path = request.getPathInfo();
    if (StringUtils.isEmpty(path)) {
      return new Path("/");
    } else {
      try {
        path = URLDecoder.decode(path, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        path = URLDecoder.decode(path);
      }
      return new Path(path);
    }

  }

  public static int safeLongToInt(long l) {
    if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          l + " cannot be cast to int without changing its value.");
    }
    return (int) l;
  }

  /**
   * <p>Check if a String ends with any of an array of specified strings.</p>
   * <p/>
   * <pre>
   * StringUtils.endsWithAny(null, null)      = false
   * StringUtils.endsWithAny(null, new String[] {"abc"})  = false
   * StringUtils.endsWithAny("abcxyz", null)     = false
   * StringUtils.endsWithAny("abcxyz", new String[] {""}) = true
   * StringUtils.endsWithAny("abcxyz", new String[] {"xyz"}) = true
   * StringUtils.endsWithAny("abcxyz", new String[] {null, "xyz", "abc"}) = true
   * </pre>
   *
   * @param string        the String to check, may be null
   * @param searchStrings the Strings to find, may be null or empty
   * @return <code>true</code> if the String ends with any of the the prefixes,
   * case insensitive, or both <code>null</code>
   * @since 2.6
   */
  public static boolean endsWithAny(String string, String[] searchStrings) {
    if (StringUtils.isEmpty(string) || ArrayUtils.isEmpty(searchStrings)) {
      return false;
    }
    for (int i = 0; i < searchStrings.length; i++) {
      String searchString = searchStrings[i];
      if (StringUtils.endsWith(string, searchString)) {
        return true;
      }
    }
    return false;
  }
}
