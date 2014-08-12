package org.apache.reef.inmemory.task;


import com.microsoft.wake.EventHandler;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.exceptions.ConnectionFailedException;
import org.apache.reef.inmemory.common.exceptions.TransferFailedException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for the loading stage. This executes block loading with
 * a thread allocated from the thread pool of the loading stage.
 * Statistics and admission control are also handled by the MemoryManager.
 */
public final class BlockLoaderExecutor implements EventHandler<BlockLoader> {

  private static final Logger LOG = Logger.getLogger(BlockLoaderExecutor.class.getName());

  private final MemoryManager memoryManager;

  @Inject
  public BlockLoaderExecutor(final MemoryManager memoryManager) {
    this.memoryManager = memoryManager;
  }

  /**
   * Load the block and update statistics.
   * This method will wait if there is too much memory pressure.
   * @param loader
   */
  @Override
  public void onNext(BlockLoader loader) {
    // TODO would it be better to report to driver?
    // It seems possible either to send an message directly or
    // to keep the failure info and send via Heartbeat

    LOG.log(Level.INFO, "Start loading block {0}", loader.getBlockId());
    final long blockSize = loader.getBlockId().getBlockSize();

    memoryManager.loadStart(blockSize);

    boolean loadSuccess;
    try {
      loader.loadBlock();

      loadSuccess = true;
      LOG.log(Level.INFO, "Finish loading block {0}", loader.getBlockId());
    } catch (ConnectionFailedException e) {
      loadSuccess = false;
      LOG.log(Level.SEVERE, "Failed to load block {0} because of connection failure", loader.getBlockId());
    } catch (TransferFailedException e) {
      loadSuccess = false;
      LOG.log(Level.SEVERE, "An error occurred while transferring the block {0} from the Datanode", loader.getBlockId());
    } catch (IOException e) {
      loadSuccess = false;
      LOG.log(Level.SEVERE, "Failed to load block "+loader.getBlockId(), e);
    }

    if (loadSuccess) {
      memoryManager.loadSuccess(blockSize, loader.isPinned());
    } else {
      memoryManager.loadFail(blockSize);
    }
  }
}