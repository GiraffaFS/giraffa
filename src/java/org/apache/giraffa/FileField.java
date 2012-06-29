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

import org.apache.giraffa.GiraffaConstants.FileState;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

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

  // Get file fields from Result
  public static long getLength(Result res) {
    return Bytes.toLong(res.getValue(getFileAttributes(), getLength()));
  }
  public static boolean getDirectory(Result res) {
    return res.containsColumn(getFileAttributes(), getDirectory());
  }
  public static short getReplication(Result res) {
    return Bytes.toShort(res.getValue(getFileAttributes(), getReplication()));
  }
  public static long getBlockSize(Result res) {
    return Bytes.toLong(res.getValue(getFileAttributes(), getBlockSize()));
  }
  public static long getMTime(Result res) {
    return Bytes.toLong(res.getValue(getFileAttributes(), getMTime()));
  }
  public static long getATime(Result res) {
    return Bytes.toLong(res.getValue(getFileAttributes(), getATime()));
  }
  public static FsPermission getPermissions(Result res) {
    return new FsPermission(
        Bytes.toShort(res.getValue(getFileAttributes(), getPermissions())));
  }
  public static String getUserName(Result res) {
    return new String(res.getValue(getFileAttributes(), getUserName()));
  }
  public static String getGroupName(Result res) {
    return new String(res.getValue(getFileAttributes(), getGroupName()));
  }
  public static byte[] getSymlink(Result res) {
    return res.getValue(getFileAttributes(), getSymlink());
  }
  public static FileState getState(Result res) {
    return FileState.valueOf(
        Bytes.toString(res.getValue(getFileAttributes(), getState())));
  }
  public static long getNsQuota(Result res) {
    return Bytes.toLong(res.getValue(getFileAttributes(), getNsQuota()));
  }
  public static long getDsQuota(Result res) {
    return Bytes.toLong(res.getValue(getFileAttributes(), getDsQuota()));
  }
}