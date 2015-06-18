package org.apache.giraffa.hbase.bootstrap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

class StaticByteInputStream extends InputStream
implements Seekable, PositionedReadable {
  InputStream in;

  public StaticByteInputStream(byte[] bytes) {
    in = new ByteArrayInputStream(bytes);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return in.read(buffer, offset, length);
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
    throw new UnsupportedOperationException("seek");
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  @Private
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }
}