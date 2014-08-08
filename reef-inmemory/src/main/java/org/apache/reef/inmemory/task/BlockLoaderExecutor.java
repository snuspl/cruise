package org.apache.reef.inmemory.task;


import com.microsoft.wake.EventHandler;
import org.apache.reef.inmemory.common.CacheStatistics;
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
  private final CacheStatistics statistics;

  @Inject
  public BlockLoaderExecutor(final CacheStatistics statistics) {
    this.statistics = statistics;
  }

  @Override
  public void onNext(BlockLoader loader) {
    // TODO would it be better to report to driver?
    // It seems possible either to send an message directly or
    // to keep the failure info and send via Heartbeat
    LOG.log(Level.INFO, "Start loading block {0}", loader.getBlockId());
    statistics.addLoadingMB(loader.getBlockId().getBlockSize());
    try {
      loader.loadBlock();

      statistics.addCacheMB(loader.getBlockId().getBlockSize());
      statistics.subtractLoadingMB(loader.getBlockId().getBlockSize());
      LOG.log(Level.INFO, "Finish loading block {0}", loader.getBlockId());
    } catch (ConnectionFailedException e) {
      statistics.subtractLoadingMB(loader.getBlockId().getBlockSize());
      LOG.log(Level.SEVERE, "Failed to load block {0} because of connection failure", loader.getBlockId());
    } catch (TokenDecodeFailedException e) {
      statistics.subtractLoadingMB(loader.getBlockId().getBlockSize());
      LOG.log(Level.SEVERE, "Failed to load block {0}, HdfsToken is not valid", loader.getBlockId());
    } catch (TransferFailedException e) {
      statistics.subtractLoadingMB(loader.getBlockId().getBlockSize());
      LOG.log(Level.SEVERE, "An error occurred while transferring the block {0} from the Datanode", loader.getBlockId());
    } catch (IOException e) {
      statistics.subtractLoadingMB(loader.getBlockId().getBlockSize());
      LOG.log(Level.SEVERE, "Unhandled Exception :", e.getCause());
    }
  }
}