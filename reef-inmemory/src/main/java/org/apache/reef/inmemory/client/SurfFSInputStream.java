package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
public final class SurfFSInputStream extends FSInputStream {

  private static final Logger LOG = Logger.getLogger(SurfFSInputStream.class.getName());

  private final FileMeta fileMeta;
  private final CacheClientManager cacheManager;
  private final List<CacheBlockLoader> blocks;

  private long pos;

  private int blockIdx;
  private int blockPos;

  public SurfFSInputStream(final FileMeta fileMeta,
                           final CacheClientManager cacheManager,
                           final Configuration conf) {
    this.fileMeta = fileMeta;
    this.cacheManager = cacheManager;
    this.blocks = new ArrayList<>(fileMeta.getBlocksSize());
    for (BlockInfo block : fileMeta.getBlocks()) {
      final LoadProgressManager progressManager = new LoadProgressManagerImpl();
      blocks.add(new CacheBlockLoader(block, this.cacheManager, progressManager, conf));
    }

    this.pos = 0;
    this.blockIdx = 0;
    this.blockPos = 0;
  }

  // TODO: the while loop may not belong here; this should just be a best-effort read? But, if we keep reading 0, things get weird
  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (position >= fileMeta.getFileSize()) {
      throw new EOFException("Read position "+position+" exceeds file size "+fileMeta.getFileSize());
    }

    LOG.log(Level.FINE, "Start read at position {0} with length {1}",
            new String[] {Long.toString(position), Integer.toString(length)});

    // seek to position
    long seekRemaining = position;
    int blockIndex = 0;
    int blockPosition = 0;
    while (seekRemaining > 0) {
      BlockInfo currBlock = fileMeta.getBlocks().get(blockIndex);
      assert(blockPosition < currBlock.getLength());
      assert(blockIndex < fileMeta.getBlocksSize());

      long toSeek = Math.min(seekRemaining, currBlock.getLength() - blockPosition);
      seekRemaining -= toSeek;
      blockPosition += toSeek;

      if (blockPosition == currBlock.getLength()) {
        blockPosition = 0;
        blockIndex++;
      }
    }

    // copy data
    int copied = 0;
    long copyRemaining = length - offset;
    while (copyRemaining > 0 && position < fileMeta.getFileSize()) {
      BlockInfo currBlock = fileMeta.getBlocks().get(blockIndex);
      assert(blockPosition < currBlock.getLength());
      assert(blockIndex < fileMeta.getBlocksSize());

      ByteBuffer data = blocks.get(blockIndex).getData(blockPosition);
      int toCopy = (int)Math.min(copyRemaining, data.remaining());
      data.get(buffer, offset + copied, toCopy);

      copyRemaining -= toCopy;
      blockPosition += toCopy;
      position += toCopy;
      copied += toCopy;

      if (blockPosition == currBlock.getLength()) {
        blocks.get(blockIndex).flushLocalCache();
        blockPosition = 0;
        blockIndex++;
      }
    }

    LOG.log(Level.FINE, "Done read at position {0} with length {1}",
            new String[] {Long.toString(position), Integer.toString(length)});
    return copied;
  }

  /**
   * Read the entire buffer.
   */
  @Override
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    if (this.pos >= fileMeta.getFileSize()) {
      return -1;
    }

    int copied = read(this.pos, buf, off, len);
    // Update position, without checking validity
    this.pos += copied;
    return copied;
  }

  @Override
  public synchronized int read() throws IOException {
    if (pos >= fileMeta.getFileSize()) {
      return -1;
    }

    ByteBuffer data = blocks.get(blockIdx).getData(blockPos);
    int val = data.get() & 0xff;

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
  public synchronized void seek(long pos) throws IOException {
    if (pos >= fileMeta.getFileSize()) {
      throw new EOFException("Seek position "+pos+" exceeds file size "+fileMeta.getFileSize());
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
