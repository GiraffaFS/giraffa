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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

/**
 * Directory in Giraffa is a row, which associates file and sub-directory names
 * contained in the directory with their row keys.
 */
public class DirectoryTable implements Serializable {
  private static final long serialVersionUID = 987654321098765432L;

  private Map<String,RowKey> childrenKeys;

  public DirectoryTable() {
    childrenKeys = new HashMap<String,RowKey>();
  }

  @SuppressWarnings("unchecked")
  public
  DirectoryTable(byte[] list) throws IOException, ClassNotFoundException {
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(list));
    try {
      childrenKeys = (HashMap<String,RowKey>) in.readObject();
    } catch (IOException e) {
      throw e;
    } catch (ClassNotFoundException e) {
      throw e;
    } finally {
    in.close();
    }
  }

  public Collection<RowKey> getEntries() {
    return Collections.unmodifiableCollection(childrenKeys.values());
  }

  int size() {
    return childrenKeys.size();
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  boolean contains(String fileName) {
    return childrenKeys.containsKey(fileName);
  }

  RowKey getEntry(String fileName) {
    return childrenKeys.get(fileName);
  }

  public boolean addEntry(RowKey child) {
    return childrenKeys.put(new Path(child.getPath()).getName(), child) == null;
  }

  public boolean removeEntry(String fileName) {
    return childrenKeys.remove(fileName) != null;
  }

  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream objOut = new ObjectOutputStream(bos);
    objOut.writeObject(childrenKeys);
    byte[] retVal = bos.toByteArray();
    objOut.close();
    return retVal;
  }
}
