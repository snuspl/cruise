package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * FSInputStream for Surf.
 * Caches intermediate chunks accessed through InputStream and Seekable seek / read calls
 *   because these are likely part of a succession of sequential reads.
 * Does not cache accesses via PositionedReadable read / readFully
 *   because these are likely random or one-pass reads.
 */
public final class SurfFSInputStream extends FSInputStream {

  private static final Logger LOG = Logger.getLogger(SurfFSInputStream.class.getName());

  private final FileMeta fileMeta;
  private final CacheClientManager cacheManager;
  private final List<CacheBlockLoader> blocks;

  private long pos;

  public SurfFSInputStream(final FileMeta fileMeta,
                           final CacheClientManager cacheManager,
                           final Configuration conf,
                           final EventRecorder recorder) {
    this.fileMeta = fileMeta;
    this.cacheManager = cacheManager;
    this.blocks = new ArrayList<>(fileMeta.getBlocksSize());
    for (BlockMeta block : fileMeta.getBlocks()) {
      final LoadProgressManager progressManager = new LoadProgressManagerImpl();
      blocks.add(new CacheBlockLoader(block, this.cacheManager, progressManager, conf, recorder));
    }

    this.pos = 0;
  }

  private int doRead(
          final long position, final byte[] buffer, final int offset, final int length, final boolean sequentialRead)
                throws IOException {
    if (position >= fileMeta.getFileSize()) {
      return -1;
    }

    LOG.log(Level.FINE, "Start read at position {0} with length {1}",
            new String[] {Long.toString(position), Integer.toString(length)});

    // Compute block-level position
    final long blockIndexLong = position / fileMeta.getBlockSize();
    if (blockIndexLong > Integer.MAX_VALUE) {
      throw new IOException("Cannot read file with more blocks than "+Integer.MAX_VALUE);
    }
    int blockIndex = (int) blockIndexLong;
    int blockPosition = (int) (position % fileMeta.getBlockSize());

    // copy data
    int numCopied = 0;
    int bufferRemaining = length - offset;
    while (bufferRemaining > 0 && position + numCopied < fileMeta.getFileSize()) {
      final BlockMeta currBlock = fileMeta.getBlocks().get(blockIndex);
      assert(blockPosition < currBlock.getLength());
      assert(blockIndex < fileMeta.getBlocksSize());

      final ByteBuffer data = blocks.get(blockIndex).getData(blockPosition, bufferRemaining, sequentialRead);
      final int numBytes = data.remaining();
      data.get(buffer, offset + numCopied, numBytes);

      bufferRemaining -= numBytes;
      blockPosition += numBytes;
      numCopied += numBytes;

      if (blockPosition == currBlock.getLength()) {
        // Clear local cache on sequential read when moving to next block
        if (sequentialRead) {
          blocks.get(blockIndex).flushLocalCache();
        }

        blockPosition = 0;
        blockIndex++;
      }
    }

    LOG.log(Level.FINE, "Done read at position {0} with length {1}",
            new String[] {Long.toString(position), Integer.toString(length)});
    return numCopied;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return doRead(position, buffer, offset, length, false);
  }

  /**
   * Read the entire buffer.
   */
  @Override
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    if (this.pos >= fileMeta.getFileSize()) {
      return -1;
    }

    int copied = doRead(this.pos, buf, off, len, true);
    // Update position, without checking validity
    this.pos += copied;
    return copied;
  }

  @Override
  public synchronized int read() throws IOException {
    if (pos >= fileMeta.getFileSize()) {
      return -1;
    }

    // Compute block-level position
    final long blockIndexLong = pos / fileMeta.getBlockSize();
    if (blockIndexLong > Integer.MAX_VALUE) {
      throw new IOException("Cannot read file with more blocks than "+Integer.MAX_VALUE);
    }
    int blockIdx = (int) blockIndexLong;
    int blockPos = (int) (pos % fileMeta.getBlockSize());

    ByteBuffer data = blocks.get(blockIdx).getData(blockPos, 1, true);
    int val = data.get() & 0xff;

    // Update position, without checking validity
    this.pos++;

    return val;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos >= fileMeta.getFileSize()) {
      throw new EOFException("Seek position "+pos+" exceeds file size "+fileMeta.getFileSize());
    }
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
