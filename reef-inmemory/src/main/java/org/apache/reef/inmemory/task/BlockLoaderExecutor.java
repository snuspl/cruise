package org.apache.reef.inmemory.task;


import com.microsoft.wake.EventHandler;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
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
   * Failures will be reported to Driver on the next Heartbeat.
   * As soon as loader is done with loadBlock it is set to null to ensure immediate availability for GC.
   * @param loader
   */
  @Override
  public void onNext(BlockLoader loader) {
    LOG.log(Level.INFO, "Start loading block {0}", loader.getBlockId());
    final BlockId blockId = loader.getBlockId();
    final boolean isPinned = loader.isPinned();

    try {
      memoryManager.loadStart(blockId);
    } catch (BlockNotFoundException e) {
      LOG.log(Level.INFO, "Already removed block {0}", blockId);
      return;
    }

    try {
      loader.loadBlock();
      loader = null;
      LOG.log(Level.INFO, "Finish loading block {0}", blockId);
    } catch (ConnectionFailedException e) {
      loader = null;
      memoryManager.loadFail(blockId, e);
      LOG.log(Level.SEVERE, "Failed to load block {0} because of connection failure", blockId);
      return;
    } catch (TransferFailedException e) {
      loader = null;
      memoryManager.loadFail(blockId, e);
      LOG.log(Level.SEVERE, "An error occurred while transferring the block {0} from the Datanode", blockId);
      return;
    } catch (IOException e) {
      loader = null;
      memoryManager.loadFail(blockId, e);
      LOG.log(Level.SEVERE, "Failed to load block "+blockId, e);
      return;
    }

    memoryManager.loadSuccess(blockId, isPinned);
  }
}