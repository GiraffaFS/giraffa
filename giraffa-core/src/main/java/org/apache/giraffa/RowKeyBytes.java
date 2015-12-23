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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.NamespaceProcessor;

public class RowKeyBytes {
  private static final String DEFAULT_ENCODING = GiraffaConstants.UTF8;
  
  private static final Log LOG =
    LogFactory.getLog(NamespaceProcessor.class.getName());

  public static byte[] toBytes(String toConv) {
    try {
      return toConv.getBytes(DEFAULT_ENCODING);
    } catch (UnsupportedEncodingException e) {
      LOG.error(DEFAULT_ENCODING + " not supported?", e);
    }
    return null;
  }
  
  // Follows the convention used in HBase Bytes for compatibility.
  public static byte[] toBytes(short toConv) {
    byte[] b = new byte[2];
    b[1] = (byte) toConv;
    b[0] = (byte) (toConv >> 8);
    return b;
  }

  // Follows the convention used in HBase Bytes for compatibality.
  public static Short toShort(byte[] toConv) {
    short n = 0;
    n ^= toConv[0] & 0xFF;
    n <<= 8;
    n ^= toConv[1] & 0xFF;
    return n;
  }

  // Follows the convention used in HBase Bytes for compatibality.
  public static long toLong(byte[] toConv, int offset) {
    assert offset + 8 <= toConv.length;
    long l = 0;
    for(int i = offset; i < offset + 8; i++) {
      l <<= 8;
      l ^= toConv[i] & 0xFF;
    }
    return l;
  }

  // Follows the convention used in HBase Bytes for compatibality.
  public static void putLong(byte[] b, int offset, long val) {
    assert offset + 8 <= b.length;
    for (int i = 7; i > 0; i--) {
      b[i + offset] = (byte) val;
      val >>>= 8;
    }
    b[offset] = (byte) val;
  }

  public static void lshift(byte[] b, int numBytes) {
    assert numBytes >= 0 && numBytes <= b.length;
    System.arraycopy(b, numBytes, b, 0, b.length - numBytes);
  }

  public static byte[] add(byte[] a, byte[] b) {
    byte [] result = new byte[a.length + b.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }

  public static String toString(byte[] b){
    if(b == null) {
      return null;
    }
    try {
      return new String(b, DEFAULT_ENCODING);
    }catch(UnsupportedEncodingException e) {
      LOG.error(DEFAULT_ENCODING + " not supported?", e);
      return null;
    }
  }

  public static String toString(final byte [] b, int off, int len) {
    if (b == null) {
      return null;
    }
    if (len == 0) {
      return "";
    }
    try {
      return new String(b, off, len, DEFAULT_ENCODING);
    } catch (UnsupportedEncodingException e) {
      LOG.error(DEFAULT_ENCODING + " not supported?", e);
      return null;
    }
  }

  /**
   * Lexicographically compare two arrays.
   *
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1,
    byte[] buffer2, int offset2, int length2) {
    byte[] sub1 = Arrays.copyOfRange(buffer1, offset1, offset1+length1);
    byte[] sub2 = Arrays.copyOfRange(buffer2, offset2, offset2+length2);
    for(int i = 0; i < sub1.length | i < sub2.length; i++) {
      if(sub1[i] == sub2[i]) 
        continue;
      else if(sub1[i] < sub2[i])
        return -1;
      else if(sub2[i] < sub1[i])
        return 1;
    }
    return 0;
  }

}
