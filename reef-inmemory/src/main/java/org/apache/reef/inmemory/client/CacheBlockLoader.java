package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lazily gets data from Cache Server, using Thrift. Data is retrieved
 * not all at once, but rather in chunks.
 *
 * The latest chunk is cached locally, until another chunk is read,
 * or the parent InputStream deems it no longer necessary and calls
 * flushLocalCache().
 */
public final class CacheBlockLoader {
  private static final Logger LOG = Logger.getLogger(CacheBlockLoader.class.getName());

  private static final long NO_OFFSET = -1;

  private final BlockInfo block;

  private final CacheClientManager cacheManager;
  private final LoadProgressManager progressManager;

  private byte[] data;
  private long offset = NO_OFFSET;

  public CacheBlockLoader(final BlockInfo block,
                          final CacheClientManager cacheManager,
                          final LoadProgressManager progressManager,
                          final Configuration conf) {
    this.block = block;

    this.cacheManager = cacheManager;
    this.progressManager = progressManager;
    this.progressManager.initialize(block.getLocations(), conf);
  }

  /**
   * This client must be used within a synchronized (client) block,
   * as other BlockLoaders may try to concurrently access the same client.
   */
  private SurfCacheService.Client getClient(final String cacheAddress) throws IOException, TTransportException {
    final SurfCacheService.Client client = cacheManager.get(cacheAddress);
    LOG.log(Level.INFO, "Connected to client at {0} for data from block {1}",
            new String[]{cacheAddress, Long.toString(block.getBlockId())});
    return client;
  }

  /**
   * Returns a ByteBuffer containing a chunk of the block, including the offset requested.
   * The amount of data returned is given with ByteBuffer.remaining() and should be read
   * starting from ByteBuffer.position().
   *
   * Includes retry on BlockLoadingException.
   *
   * In this implementation, retry is also done on BlockNotFoundException,
   * because driver does not wait until Task confirmation that block loading has been initiated.
   * This should be fixed on driver-Task communication side, once immediate communication
   * from Task to driver is implemented in REEF.
   */
  public synchronized ByteBuffer getData(long offset) throws IOException {
    long startOffset = offset - (offset % cacheManager.getBufferSize());

    LOG.log(Level.FINE, "Start get data from block {0}, with offset {1}, startOffset {2}",
            new String[]{Long.toString(block.getBlockId()), Long.toString(offset), Long.toString(startOffset)});
    if (this.offset == startOffset && this.data != null) {
      ByteBuffer dataBuffer = ByteBuffer.wrap(this.data);
      dataBuffer.position((int) offset - (int) startOffset);

      LOG.log(Level.FINE, "Done get cached data from block {0}, with offset {1}, startOffset {2}",
              new String[]{Long.toString(block.getBlockId()), Long.toString(offset), Long.toString(startOffset)});

      return dataBuffer;
    } else if (this.data != null) { // locally cached copy has different offset
      flushLocalCache();
    }

    /**
     * Retrieve the block. The cache locations for this block are tried in the order given by progressManager.
     * If the connection fails, block is unavailable, or block is still loading from the base FS at the cache node,
     * this status is reported to the progressManager. The progressManager may remove the cache as a candidate
     * or give another cache on getNextCache depending on the reported status.
     *
     * If there are no more caches remaining to try, the progressManager throws an IOException.
     */
    for ( ; ; ) {

      final String cacheAddress = progressManager.getNextCache();

      try {
        final SurfCacheService.Client client = getClient(cacheAddress);
        synchronized(client) {
          LOG.log(Level.INFO, "Start data transfer from block {0}, with startOffset {1}",
                  new String[]{Long.toString(block.getBlockId()), Long.toString(startOffset)});

          ByteBuffer dataBuffer = client.getData(block, startOffset, cacheManager.getBufferSize());
          this.data = dataBuffer.array();
          this.offset = startOffset;
          dataBuffer.position((int)offset - (int)startOffset);

          LOG.log(Level.INFO, "Done data transfer from block {0}, with startOffset {1}",
                  new String[]{Long.toString(block.getBlockId()), Long.toString(startOffset)});
          return dataBuffer;
        }
      } catch (BlockLoadingException e) {
        LOG.log(Level.FINE, "BlockLoadingException at "+cacheAddress+" loaded "+e.getBytesLoaded());
        progressManager.loadingProgress(cacheAddress, e.getBytesLoaded());
        try {
          Thread.sleep(cacheManager.getRetriesInterval());
        } catch (InterruptedException ie) {
          LOG.log(Level.WARNING, "Sleep interrupted", ie);
        }
      } catch (BlockNotFoundException e) {
        LOG.log(Level.INFO, "BlockNotFoundException at "+System.currentTimeMillis());
        progressManager.notFound(cacheAddress);
        try {
          Thread.sleep(cacheManager.getRetriesInterval());
        } catch (InterruptedException ie) {
          LOG.log(Level.WARNING, "Sleep interrupted", ie);
        }
      } catch (TException e) {
        LOG.log(Level.SEVERE, "TException", e);
        progressManager.notConnected(cacheAddress);
      }
    }
  }

  public synchronized void flushLocalCache() {
    this.offset = NO_OFFSET;
    this.data = null;
  }
}
