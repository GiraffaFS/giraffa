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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

public class TestDirectoryTable {

  @Test
  public void testDirectory() throws IOException, ClassNotFoundException {
    DirectoryTable dirTable = new DirectoryTable();
    assertTrue(dirTable.isEmpty());

    assertTrue(dirTable.addEntry(new FullPathRowKey("/abc")));
    assertTrue(dirTable.addEntry(new FullPathRowKey("/def")));
    assertTrue(dirTable.addEntry(new FullPathRowKey("/ghi")));
    byte[] out = dirTable.toBytes();

    //dirTable serialized

    dirTable = new DirectoryTable(out);
    assertEquals(dirTable.getEntries().size(), 3);
    assertTrue(dirTable.contains("abc"));
    assertTrue(dirTable.contains("def"));
    assertTrue(dirTable.contains("ghi"));
    assertEquals("/abc", dirTable.getEntry("abc").getPath().toString());
    assertEquals("/def", dirTable.getEntry("def").getPath().toString());
    assertEquals("/ghi", dirTable.getEntry("ghi").getPath().toString());

    assertFalse(dirTable.addEntry(new FullPathRowKey("/abc")));
    
    //remove entries
    assertTrue(dirTable.removeEntry("abc"));
    assertEquals(dirTable.getEntries().size(), 2);

    assertTrue(dirTable.removeEntry("def"));
    assertEquals(dirTable.getEntries().size(), 1);

    assertTrue(dirTable.removeEntry("ghi"));
    assertEquals(dirTable.getEntries().size(), 0);

    assertTrue(dirTable.isEmpty());
  }
}
