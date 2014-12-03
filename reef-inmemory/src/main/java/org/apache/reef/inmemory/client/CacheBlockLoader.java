package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.instrumentation.Event;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
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
  private final EventRecorder RECORD;

  private static final long NOT_CACHED = -1;

  private final BlockInfo block;

  private final CacheClientManager cacheManager;
  private final LoadProgressManager progressManager;

  private byte[] cachedChunk;
  private long cachedChunkPosition = NOT_CACHED;

  public CacheBlockLoader(final BlockInfo block,
                          final CacheClientManager cacheManager,
                          final LoadProgressManager progressManager,
                          final Configuration conf,
                          final EventRecorder recorder) {
    this.block = block;

    this.cacheManager = cacheManager;
    this.progressManager = progressManager;
    this.progressManager.initialize(block.getLocations(), conf);
    this.RECORD = recorder;
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
   * Returns a ByteBuffer containing a chunk of the block, including the position requested.
   * The amount of data returned is given with ByteBuffer.remaining() and should be read
   * starting from ByteBuffer.position().
   *
   * Includes retry on BlockLoadingException.
   *
   * In this implementation, retry is also done on BlockNotFoundException,
   * because Driver does not wait until Task confirmation that block loading has been initiated.
   * This should be fixed on Driver-Task communication side, once immediate communication
   * from Task to driver is implemented in REEF.
   *
   * Sequential reads are cached, while random reads are not
   *
   * @param position Get data from this position within the block
   * @param length Length of data to get. At most length bytes will be returned.
   * @param sequentialRead Whether this request is part of a sequential read. If so, local caching will be used.
   * @return A ByteBuffer, to be read from position to limit. The number of bytes will be at most as the length parameter.
   * @throws IOException
   */
  public synchronized ByteBuffer getData(final int position, final int length, final boolean sequentialRead) throws IOException {
    final int chunkPosition = position % cacheManager.getBufferSize();
    final int chunkStartPosition = position - chunkPosition;

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Start get data from block {0}, with position {1}, chunkStartPosition {2}",
              new String[]{Long.toString(block.getBlockId()), Long.toString(position), Long.toString(chunkStartPosition)});
    }
    if (this.cachedChunkPosition == chunkStartPosition && this.cachedChunk != null) {
      final ByteBuffer dataBuffer = ByteBuffer.wrap(this.cachedChunk);
      if (chunkPosition + length < dataBuffer.limit()) {
        dataBuffer.limit(chunkPosition + length);
      }
      dataBuffer.position(chunkPosition);

      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.FINEST, "Done get cached data from block {0}, with position {1}, chunkStartPosition {2}",
                new String[]{Long.toString(block.getBlockId()), Long.toString(position), Long.toString(chunkStartPosition)});
      }

      return dataBuffer;
    } else if (sequentialRead && this.cachedChunk != null) { // locally cached copy has different position
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
          final Event dataTransferEvent = RECORD.event("client.data-transfer",
                  block.getBlockId() + ":" + chunkStartPosition).start();
          LOG.log(Level.INFO, "Start data transfer from block {0}, with chunkStartPosition {1}",
                  new String[]{Long.toString(block.getBlockId()), Integer.toString(chunkStartPosition)});

          final ByteBuffer dataBuffer = client.getData(block, chunkStartPosition, cacheManager.getBufferSize());
          if (chunkPosition + length < dataBuffer.limit()) {
            dataBuffer.limit(chunkPosition + length);
            // If there is data left to be read, and this is a sequentialRead, cache it
            if (sequentialRead) {
              this.cachedChunk = dataBuffer.array();
              this.cachedChunkPosition = chunkStartPosition;
            }
          }
          dataBuffer.position(chunkPosition);

          LOG.log(Level.INFO, "Done data transfer from block {0}, with chunkStartPosition {1}",
                  new String[]{Long.toString(block.getBlockId()), Integer.toString(chunkStartPosition)});
          RECORD.record(dataTransferEvent.stop());
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

  /**
   * Flush the current locally cached chunk.
   * Should only be called by a sequential read, when the client
   * is sure that the chunk is no longer needed (e.g. at the end of a block).
   */
  public synchronized void flushLocalCache() {
    this.cachedChunkPosition = NOT_CACHED;
    this.cachedChunk = null;
  }
}
