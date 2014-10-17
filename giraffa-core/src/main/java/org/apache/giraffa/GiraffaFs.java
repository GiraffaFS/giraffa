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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * AbstractFileSystem implementation of Giraffa that delegates all calls to
 * GiraffaFileSystem. This makes Giraffa accessible using the FileContext API
 * and applications that use that API, such as Yarn. To enable, set the
 * fs.AbstractFileSystem.grfa.impl property to this class in core-site.xml.
 */
public class GiraffaFs extends DelegateToFileSystem {
  public GiraffaFs(final URI theUri, final Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new GiraffaFileSystem(), conf,
        GiraffaConfiguration.GRFA_URI_SCHEME, false);
  }

  @Override
  public int getUriDefaultPort() { // AbstractFileSystem
    return -1; // no default port for grfa:///
  }
}
