package org.apache.giraffa;

public class RenameState {
  private final boolean flag;
  private final byte[] src;

  private RenameState(boolean flag, byte[] src) {
    this.flag = flag;
    this.src = src;
  }

  public boolean getFlag() {
    return this.flag;
  }

  public byte[] getSrc() {
    return this.src;
  }

  public static RenameState TRUE(byte[] src) {
    return new RenameState(true, src);
  }

  public static RenameState FALSE() {
    return new RenameState(false, null);
  }
}
