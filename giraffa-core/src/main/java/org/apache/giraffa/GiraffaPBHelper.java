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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraffa.GiraffaProtos.UnlocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfosProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

/**
 * Helper class, similar to PBHelper, for converting between Giraffa objects
 * and their protos. Also contains serialization/deserialization helpers.
 */
public class GiraffaPBHelper {

  private static UnlocatedBlockProto convert(UnlocatedBlock toConv) {
    UnlocatedBlockProto.Builder builder = UnlocatedBlockProto.newBuilder();
    builder.setBlockToken(PBHelper.convert(toConv.getBlockToken()));
    builder.setB(PBHelper.convert(toConv.getBlock()));
    builder.setCorrupt(toConv.isCorrupt());
    builder.setOffset(toConv.getStartOffset()).build();
    return builder.build();
  }
  
  private static UnlocatedBlock convert(UnlocatedBlockProto toConv) {
    UnlocatedBlock blk = new UnlocatedBlock(
        PBHelper.convert(toConv.getB()),
        toConv.getOffset(),
        toConv.getCorrupt());
    blk.setBlockToken(PBHelper.convert(toConv.getBlockToken()));
    return blk;
  }
  
  private static DatanodeInfosProto convert(DatanodeInfo[] toConv) {
    return DatanodeInfosProto.newBuilder()
        .addAllDatanodes(PBHelper.convert(toConv))
        .build();
  }
  
  private static DatanodeInfo[] convert(DatanodeInfosProto toConv) {
    return PBHelper.convert(toConv);
  }
  
  /**
   * Serializes a list of UnlocatedBlocks into a byte array
   * @param blocks
   * @return
   * @throws IOException
   */
  public static byte[] unlocatedBlocksToBytes(List<UnlocatedBlock> blocks)
      throws IOException {
    if(blocks == null) return null;
    byte[] retVal = null;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    try {
      for(UnlocatedBlock blk : blocks) {
        convert(blk).writeDelimitedTo(out);
      }
      retVal = baos.toByteArray();
    } finally {
      out.close();
    }
    return retVal;
  }
  
  /**
   * Deserializes a byte array into a list of UnlocatedBlocks
   * @param bytes
   * @return
   * @throws IOException
   */
  public static List<UnlocatedBlock> bytesToUnlocatedBlocks(byte[] bytes)
      throws IOException {
    List<UnlocatedBlock> blocks = new ArrayList<UnlocatedBlock>();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    while(in.available() > 0){
      UnlocatedBlock blk = convert(UnlocatedBlockProto.parseDelimitedFrom(in));
      blocks.add(blk);
    }
    in.close();
    return blocks;
  }
  
  /**
   * Serializes a list of block locations (DatanodeInfo[]s) into a byte array
   * @param locations
   * @return
   * @throws IOException
   */
  public static byte[] blockLocationsToBytes(List<DatanodeInfo[]> locations)
      throws IOException {
    if(locations == null) return null;
    byte[] retVal = null;
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    try {
      for(DatanodeInfo[] locs : locations) {
        convert(locs).writeDelimitedTo(out);
      }
      retVal = baos.toByteArray();
    } finally {
      try {
        out.close();
      } catch (IOException ignored) {}
    }
    return retVal;
  }
  
  /**
   * Deserializes a byte array into a list of block locations (DatanodeInfo[]s)
   * @param bytes
   * @return
   * @throws IOException
   */
  public static List<DatanodeInfo[]> bytesToBlockLocations(byte[] bytes)
      throws IOException {
    List<DatanodeInfo[]> locs = new ArrayList<DatanodeInfo[]>();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    while(in.available() > 0){
      locs.add(convert(DatanodeInfosProto.parseDelimitedFrom(in)));
    }
    in.close();
    return locs;
  }
}
