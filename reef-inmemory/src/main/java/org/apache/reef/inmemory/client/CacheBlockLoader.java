package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
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
  private final Iterator<String> locations;

  private final CacheClientManager cacheManager;
  private SurfCacheService.Client client;

  private byte[] data;
  private long offset = NO_OFFSET;

  public CacheBlockLoader(final BlockInfo block,
                          final CacheClientManager cacheManager) {
    this.block = block;
    this.locations = block.getLocationsIterator();

    this.cacheManager = cacheManager;
  }

  /**
   * This client must be used within a synchronized (client) block,
   * as other BlockLoaders may try to concurrently access the same client.
   */
  private SurfCacheService.Client getNextClient() throws IOException {
    if (locations == null || locations.hasNext()) {
      try {
        return cacheManager.get(locations.next());
      } catch (TTransportException e) {
        LOG.log(Level.SEVERE, "TException "+e);
        throw new IOException("TTransportException");
      }
    } else {
      throw new IOException("No more locations available. LocationsIterator: "+locations);
    }
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

    if (this.offset == startOffset && this.data != null) {
      ByteBuffer dataBuffer = ByteBuffer.wrap(this.data);
      dataBuffer.position((int) offset - (int) startOffset);
      return dataBuffer;
    } else if (this.data != null) { // locally cached copy has different offset
      flushLocalCache();
    }

    if (this.client == null) {
      client = getNextClient();
    }

    LOG.log(Level.INFO, "Sending block request: "+block+", with startOffset "+startOffset);
    for (int i = 0; i < 1 + cacheManager.getRetries(); i++) {
      try {
        synchronized(client) {
          ByteBuffer dataBuffer = client.getData(block, startOffset, cacheManager.getBufferSize());
          this.data = dataBuffer.array();
          this.offset = startOffset;
          dataBuffer.position((int)offset - (int)startOffset);
          return dataBuffer;
        }
      } catch (BlockLoadingException e) {
        if (i < cacheManager.getRetries()) {
          LOG.log(Level.FINE, "BlockLoadingException, load started: "+e.getTimeStarted());
          try {
            Thread.sleep(cacheManager.getRetriesInterval());
          } catch (InterruptedException ie) {
            LOG.log(Level.WARNING, "Sleep interrupted: "+ie);
          }
        }
      } catch (BlockNotFoundException e) {
        if (i < cacheManager.getRetries()) {
          LOG.log(Level.FINE, "BlockNotFoundException at "+System.currentTimeMillis());
          try {
            Thread.sleep(cacheManager.getRetriesInterval());
          } catch (InterruptedException ie) {
            LOG.log(Level.WARNING, "Sleep interrupted: "+ie);
          }
        }
      } catch (TException e) {
        LOG.log(Level.SEVERE, "TException "+e);
        throw new IOException("TException");
      }
    }
    LOG.log(Level.WARNING, "Exception after "+(1 + cacheManager.getRetries())+" tries. Aborting.");
    throw new IOException();
  }

  public synchronized void flushLocalCache() {
    this.offset = NO_OFFSET;
    this.data = null;
  }
}
