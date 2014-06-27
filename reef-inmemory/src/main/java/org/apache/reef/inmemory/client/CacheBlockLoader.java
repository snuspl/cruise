package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.fs.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.fs.service.SurfCacheService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CacheBlockLoader {
  private static final Logger LOG = Logger.getLogger(CacheBlockLoader.class.getName());

  private final BlockInfo block;
  private final Iterator<String> locations;

  private final CacheClientManager cacheManager;

  private byte[] data;

  public CacheBlockLoader(final BlockInfo block,
                          final CacheClientManager cacheManager) {
    this.block = block;
    this.locations = block.getLocationsIterator();

    this.cacheManager = cacheManager;
  }

  private SurfCacheService.Client getNextClient() throws IOException {
    if (locations.hasNext()) {
      try {
        return cacheManager.get(locations.next());
      } catch (TTransportException e) {
        LOG.log(Level.SEVERE, "TException "+e);
        throw new IOException("TTransportException");
      }
    } else {
      throw new IOException("No more locations available.");
    }
  }

  /**
   * Includes retry on BlockLoadingException.
   *
   * In this implementation, retry is also done on BlockNotFoundException,
   * because Driver does not wait until Task confirmation that block loading has been initiated.
   * This should be fixed on Driver-Task communication side.
   */
  public byte[] getBlock() throws IOException {
    if (data != null) {
      return data;
    }

    SurfCacheService.Client client = getNextClient();

    LOG.log(Level.INFO, "Sending block request: "+block);
    for (int i = 0; i < 1 + cacheManager.getRetries(); i++) {
      try {
        synchronized (client) {
          this.data = client.getData(block).array();
          return this.data;
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
}
