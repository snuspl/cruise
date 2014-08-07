package org.apache.reef.inmemory.task;


import com.microsoft.wake.EventHandler;
import org.apache.reef.inmemory.common.exceptions.ConnectionFailedException;
import org.apache.reef.inmemory.common.exceptions.TransferFailedException;
import org.apache.reef.inmemory.task.hdfs.TokenDecodeFailedException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for the loading stage. This executes block loading with
 * a thread allocated from the thread pool of the loading stage.
 */
public final class BlockLoaderExecutor implements EventHandler<BlockLoader> {

  private static final Logger LOG = Logger.getLogger(BlockLoaderExecutor.class.getName());
  private final InMemoryCache cache;

  @Inject
  BlockLoaderExecutor(final InMemoryCache cache) {
    this.cache = cache;
  }

  @Override
  public void onNext(BlockLoader loader) {
    // TODO would it be better to report to driver?
    // It seems possible either to send an message directly or
    // to keep the failure info and send via Heartbeat
    try {
      cache.load(loader);
      LOG.log(Level.INFO, "Finish loading block");
    } catch (ConnectionFailedException e) {
      LOG.log(Level.SEVERE, "Failed to load block {0} because of connection failure", loader.getBlockId());
    } catch (TokenDecodeFailedException e) {
      LOG.log(Level.SEVERE, "Failed to load block {0}, HdfsToken is not valid", loader.getBlockId());
    } catch (TransferFailedException e) {
      LOG.log(Level.SEVERE, "An error occurred while transferring the block {0} from the Datanode", loader.getBlockId());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Unhandled Exception :", e.getCause());
    }
  }
}