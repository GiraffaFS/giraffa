package org.apache.giraffa;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

public class UnlocatedBlock {

  private ExtendedBlock b;
  private long offset;  // offset of the first byte of the block in the file
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and 
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken = new Token<BlockTokenIdentifier>();

  public UnlocatedBlock(LocatedBlock b) {
    this(b.getBlock(), b.getStartOffset(), b.isCorrupt());
    this.blockToken = b.getBlockToken();
  }

  public UnlocatedBlock(ExtendedBlock b, long startOffset, boolean corrupt) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
  }

  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
  }

  public ExtendedBlock getBlock() {
    return b;
  }

  public long getStartOffset() {
    return offset;
  }

  public long getBlockSize() {
    return b.getNumBytes();
  }

  public void setBlockToken(Token<BlockTokenIdentifier> token) {
    this.blockToken = token;
  }

  public void setStartOffset(long value) {
    this.offset = value;
  }

  public void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }

  public boolean isCorrupt() {
    return this.corrupt;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b
        + "; getBlockSize()=" + getBlockSize()
        + "; corrupt=" + corrupt
        + "; offset=" + offset
        + "}";
  }

  public LocatedBlock toLocatedBlock(DatanodeInfo[] locs) {
    LocatedBlock blk = new LocatedBlock(this.getBlock(), locs,
        this.getStartOffset(), this.isCorrupt());
    blk.setBlockToken(this.getBlockToken());
    return blk;
  }

  public static List<LocatedBlock> toLocatedBlocks(List<UnlocatedBlock> blocks,
      List<DatanodeInfo[]> locs) {
    List<LocatedBlock> locatedBlocks = new ArrayList<LocatedBlock>();
    int size = blocks.size();
    for(int i = 0; i < size; i++) {
      locatedBlocks.add(blocks.get(i).toLocatedBlock(locs.get(i)));
    }
    return locatedBlocks;
  }
}
