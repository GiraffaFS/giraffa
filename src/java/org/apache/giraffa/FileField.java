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

public enum FileField {
  REPLICATION ("replication".getBytes()),
  USER_NAME ("userName".getBytes()),
  GROUP_NAME ("groupName".getBytes()),
  LENGTH ("length".getBytes()),
  DS_QUOTA ("dsQuota".getBytes()),
  NS_QUOTA ("nsQuota".getBytes()),
  M_TIME ("mtime".getBytes()),
  A_TIME ("atime".getBytes()),
  PERMISSIONS ("permissions".getBytes()),
  NAME ("src".getBytes()),
  STATE ("state".getBytes()),
  ACTION ("action".getBytes()),
  SYMLINK ("symlink".getBytes()),
  DIRECTORY ("directory".getBytes()),
  BLOCK_SIZE ("blockSize".getBytes()),
  BLOCK ("block".getBytes()),
  FILE_ATTRIBUTES ("default".getBytes());

  private byte[] bytes = null;
  private FileField(byte[] arg) {this.bytes = arg;}
  public byte[] getBytes() {return bytes;}

  public static byte[] getReplication() {
    return REPLICATION.bytes;
  }
  public static byte[] getNsQuota() {
    return NS_QUOTA.bytes;
  }
  public static byte[] getMTime() {
    return M_TIME.bytes;
  }
  public static byte[] getATime() {
    return A_TIME.bytes;
  }
  public static byte[] getPermissions() {
    return PERMISSIONS.bytes;
  }
  public static byte[] getFileName() {
    return NAME.bytes;
  }
  public static byte[] getDirectory() {
    return DIRECTORY.bytes;
  }
  public static byte[] getBlockSize() {
    return BLOCK_SIZE.bytes;
  }
  public static byte[] getFileAttributes() {
    return FILE_ATTRIBUTES.bytes;
  }
  public static byte[] getDsQuota() {
    return DS_QUOTA.bytes;
  }
  public static byte[] getGroupName() {
    return GROUP_NAME.bytes;
  }
  public static byte[] getUserName() {
    return USER_NAME.bytes;
  }
  public static byte[] getBlock() {
    return BLOCK.bytes;
  }
  public static byte[] getSymlink() {
    return SYMLINK.bytes;
  }
  public static byte[] getState() {
    return STATE.bytes;
  }
  public static byte[] getAction() {
    return ACTION.bytes;
  }
  public static byte[] getLength() {
    return LENGTH.bytes;
  }
}
