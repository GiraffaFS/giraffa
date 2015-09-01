package org.apache.giraffa.hbase.bootstrap;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

class StaticByteInputStream extends ByteArrayInputStream
implements Seekable, PositionedReadable {

  public StaticByteInputStream(byte[] bytes) {
    super(bytes);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return read(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    this.read(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void seek(long pos) throws IOException {
    this.pos = (int) pos;
  }

  @Override
  public long getPos() throws IOException {
    return this.pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int read() {
    return super.read();
  }
}