package org.apache.reef.inmemory.task;


import com.microsoft.wake.EventHandler;
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
   * @param loader
   */
  @Override
  public void onNext(BlockLoader loader) {
    LOG.log(Level.INFO, "Start loading block {0}", loader.getBlockId());
    final BlockId blockId = loader.getBlockId();
    final long blockSize = loader.getBlockId().getBlockSize();

    memoryManager.loadStart(blockSize);

    try {
      loader.loadBlock();

      LOG.log(Level.INFO, "Finish loading block {0}", loader.getBlockId());
    } catch (ConnectionFailedException e) {
      memoryManager.loadFail(blockId, e);
      LOG.log(Level.SEVERE, "Failed to load block {0} because of connection failure", loader.getBlockId());
      return;
    } catch (TransferFailedException e) {
      memoryManager.loadFail(blockId, e);
      LOG.log(Level.SEVERE, "An error occurred while transferring the block {0} from the Datanode", loader.getBlockId());
      return;
    } catch (IOException e) {
      memoryManager.loadFail(blockId, e);
      LOG.log(Level.SEVERE, "Failed to load block "+loader.getBlockId(), e);
      return;
    }

    memoryManager.loadSuccess(blockSize, loader.isPinned());
  }
}