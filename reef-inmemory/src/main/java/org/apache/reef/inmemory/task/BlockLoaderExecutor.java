package org.apache.reef.inmemory.task;


import com.google.common.cache.Cache;
import com.microsoft.wake.EventHandler;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.ConnectionFailedException;
import org.apache.reef.inmemory.common.exceptions.MemoryLimitException;
import org.apache.reef.inmemory.common.exceptions.TransferFailedException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for the loading stage. This executes block loading with
 * a thread allocated from the thread pool of the loading stage.
 * Statistics and admission control are also handled by the MemoryManager.
 */
public final class BlockLoaderExecutor implements EventHandler<BlockLoader> {

  private static final Logger LOG = Logger.getLogger(BlockLoaderExecutor.class.getName());

  private final Cache<BlockId, BlockLoader> cache;
  private final MemoryManager memoryManager;

  @Inject
  public BlockLoaderExecutor(final Cache<BlockId, BlockLoader> cache,
                             final MemoryManager memoryManager) {
    this.cache = cache;
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
    final boolean pin = loader.isPinned();

    try {
      boolean needSpace = true;
      while (needSpace) {
        final List<BlockId> evictionList = memoryManager.loadStart(blockId, pin);
        needSpace = (evictionList != null);
        if (needSpace) {
          for (final BlockId toEvict : evictionList) {
            LOG.log(Level.INFO, toEvict+" eviction request being made");
            cache.invalidate(toEvict);
          }
        }
      }
    } catch (BlockNotFoundException e) {
      LOG.log(Level.INFO, "Already removed block {0}", blockId);
      return;
    } catch (MemoryLimitException e) {
      LOG.log(Level.SEVERE, "Memory limit reached", e);
      memoryManager.loadStartFail(blockId, pin, e);
      return;
    }

    try {
      loader.loadBlock();
      loader = null;
      LOG.log(Level.INFO, "Finish loading block {0}", blockId);
    } catch (ConnectionFailedException e) {
      loader = null;
      LOG.log(Level.SEVERE, "Failed to load block {0} because of connection failure", blockId);
      memoryManager.loadFail(blockId, pin, e);
      return;
    } catch (TransferFailedException e) {
      loader = null;
      LOG.log(Level.SEVERE, "An error occurred while transferring the block {0} from the Datanode", blockId);
      memoryManager.loadFail(blockId, pin, e);
      return;
    } catch (IOException e) {
      loader = null;
      LOG.log(Level.SEVERE, "Failed to load block "+blockId, e);
      memoryManager.loadFail(blockId, pin, e);
      return;
    }

    memoryManager.loadSuccess(blockId, pin);
  }
}
