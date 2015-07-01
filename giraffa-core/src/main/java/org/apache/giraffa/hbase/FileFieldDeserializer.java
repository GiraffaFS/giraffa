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

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import org.apache.giraffa.FileField;
import org.apache.giraffa.FileLease;
import org.apache.giraffa.GiraffaConstants;
import org.apache.giraffa.GiraffaPBHelper;
import org.apache.giraffa.GiraffaProtos;
import org.apache.giraffa.RenameState;
import org.apache.giraffa.RowKeyBytes;
import org.apache.giraffa.UnlocatedBlock;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * A utility class to deserialize file fields from a table row {@link Result}.
 */
public class FileFieldDeserializer {
  public static List<UnlocatedBlock> getBlocks(Result res) throws IOException {
    byte[] value = res.getValue(
        FileField.getFileAttributes(), FileField.getBlock());
    return GiraffaPBHelper.bytesToUnlocatedBlocks(value);
  }

  public static List<DatanodeInfo[]> getLocations(Result res)
      throws IOException {
    byte[] value = res.getValue(
        FileField.getFileAttributes(), FileField.getLocations());
    return GiraffaPBHelper.bytesToBlockLocations(value);
  }

  public static boolean getDirectory(Result res) {
    return res.containsColumn(FileField.getFileAttributes(),
        FileField.getDirectory());
  }

  public static short getReplication(Result res) {
    return Bytes.toShort(res.getValue(FileField.getFileAttributes(),
        FileField.getReplication()));
  }

  public static long getBlockSize(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(),
        FileField.getBlockSize()));
  }

  public static long getMTime(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(),
        FileField.getMTime()));
  }

  public static long getATime(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(),
        FileField.getATime()));
  }

  public static FsPermission getPermissions(Result res) {
    return new FsPermission(Bytes.toShort(res.getValue(
        FileField.getFileAttributes(), FileField.getPermissions())));
  }

  public static String getFileName(Result res) {
    return Bytes.toString(res.getValue(FileField.getFileAttributes(),
        FileField.getFileName()));
  }

  public static String getUserName(Result res) {
    return Bytes.toString(res.getValue(FileField.getFileAttributes(),
        FileField.getUserName()));
  }

  public static String getGroupName(Result res) {
    return RowKeyBytes.toString(res.getValue(FileField.getFileAttributes(),
        FileField.getGroupName()));
  }

  public static byte[] getSymlink(Result res) {
    return res.getValue(FileField.getFileAttributes(), FileField.getSymlink());
  }

  public static GiraffaConstants.FileState getFileState(Result res) {
    return GiraffaConstants.FileState.valueOf(Bytes.toString(res.getValue(
        FileField.getFileAttributes(), FileField.getFileState())));
  }

  public static RenameState getRenameState(Result res) throws IOException {
    return GiraffaPBHelper.convert(GiraffaProtos.RenameStateProto.parseFrom(
      res.getValue(FileField.getFileAttributes(), FileField.getRenameState())));
  }

  public static long getNsQuota(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(),
        FileField.getNsQuota()));
  }

  public static long getDsQuota(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(),
        FileField.getDsQuota()));
  }

  public static long getLength(Result res) {
    return Bytes.toLong(res.getValue(FileField.getFileAttributes(),
        FileField.getLength()));
  }

  public static FileLease getLease(Result res) throws IOException {
    if(getDirectory(res))
      return null;
    byte[] leaseByteArray = res.getValue(FileField.getFileAttributes(),
        FileField.getLease());
    if(leaseByteArray == null || leaseByteArray.length == 0)
      return null;
    return GiraffaPBHelper.bytesToHdfsLease(leaseByteArray);
  }

  public static List<XAttr> listXAttrs(Result res) {
    NavigableMap<byte[], byte[]> map =
            res.getFamilyMap(FileField.getFileExtenedAttributes());
    List<XAttr> resList = Lists.newArrayListWithCapacity(map.size());

    for(Entry<byte[], byte[]> entry: map.entrySet()) {
        XAttr xAttr = XAttrHelper.buildXAttr(Bytes.toString(entry.getKey()),
                entry.getValue());

        resList.add(xAttr);
    }
    return resList;
  }
}
