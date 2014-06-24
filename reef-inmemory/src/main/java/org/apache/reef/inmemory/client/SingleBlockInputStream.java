package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

public class SingleBlockInputStream extends ByteArrayInputStream
        implements Seekable, PositionedReadable {

  private long pos;

  public SingleBlockInputStream(byte[] data) {
    super(data);
    this.pos = 0;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    synchronized(this) {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } finally {
        seek(oldPos);
      }
      return nread;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = read(position+nread, buffer, offset+nread, length-nread);
      if (nbytes < 0) {
        throw new EOFException("Reached end of file before read fully completed");
      }
      nread += nbytes;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void seek(long pos) throws IOException {
    reset();
    skip(pos);
    this.pos = pos;
  }

  @Override
  public long getPos() throws IOException {
    return this.pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}
