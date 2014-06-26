package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.FileMeta;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Input stream implementation that works with blocks that have
 * already been transfered. Need to move to an on-demand version.
 *
 * However, not sure whether to implement the on-demand version
 * using Thrift's client implementation, or to move to a different
 * method of retrieving blocks.
 * The Thrift client returns a ByteBuffer and the read()
 * methods pass in a buffer. Thus, we end up copying the contents of
 * the ByteBuffer into the passed in buffer which is inefficient.
 */
public class SurfFSInputStream extends InputStream
        implements Seekable, PositionedReadable {

  private static final Logger LOG = Logger.getLogger(SurfFSInputStream.class.getName());

  private final FileMeta fileMeta;
  private final List<ByteBuffer> blockBuffers;

  private long pos;

  private int blockIdx;
  private int blockPos;

  public SurfFSInputStream(FileMeta fileMeta, List<ByteBuffer> blockBuffers) {
    this.fileMeta = fileMeta;
    this.blockBuffers = blockBuffers;

    this.pos = 0;
    this.blockIdx = 0;
    this.blockPos = 0;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (position >= fileMeta.getFileSize()) {
      throw new IOException("Read position "+position+" exceeds file size "+fileMeta.getFileSize());
    }

    int copied = 0;

    // seek to position
    long remaining = position;
    int blockIndex = 0;
    int blockPosition = 0;
    while (remaining > 0 && position < fileMeta.getFileSize()) {
      BlockInfo currBlock = fileMeta.getBlocks().get(blockIndex);
      assert(blockPosition < currBlock.getLength());

      long toSeek = Math.min(remaining, currBlock.getLength() - blockPosition);
      remaining -= toSeek;
      blockPosition += toSeek;
      position += toSeek;

      if (blockPosition == currBlock.getLength()) {
        blockPosition = 0;
        blockIndex++;
      }
    }

    // copy data
    int bufferIndex = offset;
    remaining = length;
    while (remaining > 0 && position < fileMeta.getFileSize()) {
      BlockInfo currBlock = fileMeta.getBlocks().get(blockIndex);
      assert(blockPosition < currBlock.getLength());

      long toCopy = Math.min(remaining, currBlock.getLength() - blockPosition);
      for (int i = 0; i < toCopy; i++) {
        buffer[((int) (position + i))] = blockBuffers.get(blockIndex).get(blockPosition + i);
      }
      remaining -= toCopy;
      blockPosition += toCopy;
      position += toCopy;
      copied += toCopy;

      if (blockPosition == currBlock.getLength()) {
        blockPosition = 0;
        blockIndex++;
      }
    }
    return copied;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    if (length + offset >= fileMeta.getFileSize()) {
      throw new IOException("Length "+length+ " + offset "+offset+" exceeds file size "+fileMeta.getFileSize());
    }

    read(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public int read() throws IOException {
    if (pos >= fileMeta.getFileSize()) {
      return -1;
    }

    int val = blockBuffers.get(blockIdx).get(blockPos) & 0xff;

    // Update position, without checking validity
    this.pos++;
    this.blockPos++;
    if (this.blockPos == fileMeta.getBlocks().get(blockIdx).getLength()) {
      this.blockPos = 0;
      this.blockIdx++;
    }

    return val;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos >= fileMeta.getFileSize()) {
      throw new IOException("Seek position "+pos+" exceeds file size "+fileMeta.getFileSize());
    }
    if (pos < this.pos) { // reset position
      this.pos = 0;
      blockIdx = 0;
      blockPos = 0;
    }

    long remaining = pos - this.pos;
    while (remaining > 0 && this.pos < fileMeta.getFileSize()) {
      BlockInfo currBlock = fileMeta.getBlocks().get(blockIdx);
      assert(blockPos < currBlock.getLength());

      long toSeek = Math.min(remaining, currBlock.getLength() - blockPos);
      remaining -= toSeek;
      blockPos += toSeek;
      this.pos += toSeek;

      if (blockPos == currBlock.getLength()) {
        blockPos = 0;
        blockIdx++;
      }
    }
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
