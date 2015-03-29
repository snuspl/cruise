package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.task.write.BlockReceiver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.wake.EStage;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.BlockNotWritableException;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public final class InMemoryCacheImpl implements InMemoryCache {
  private final Logger LOG = Logger.getLogger(InMemoryCacheImpl.class.getName());

  private final MemoryManager memoryManager;
  private final LRUEvictionManager lru;
  private final EStage<BlockLoader> loadingStage;
  private final int loadingBufferSize;

  private final Cache<BlockId, CacheEntry> cache;
  private final CacheAdmissionController cacheAdmissionController;
  private final HeartBeatTriggerManager heartBeatTriggerManager;

  @Inject
  public InMemoryCacheImpl(final Cache<BlockId, CacheEntry> cache,
                           final MemoryManager memoryManager,
                           final CacheAdmissionController cacheAdmissionController,
                           final LRUEvictionManager lru,
                           final EStage<BlockLoader> loadingStage,
                           final @Parameter(CacheParameters.NumServerThreads.class) int numThreads,
                           final @Parameter(CacheParameters.LoadingBufferSize.class) int loadingBufferSize,
                           final HeartBeatTriggerManager heartBeatTriggerManager) {
    this.cache = cache;
    this.memoryManager = memoryManager;
    this.cacheAdmissionController = cacheAdmissionController;
    this.lru = lru;
    this.loadingStage = loadingStage;
    this.loadingBufferSize = loadingBufferSize;
    this.heartBeatTriggerManager = heartBeatTriggerManager;
  }

  @Override
  public byte[] get(final BlockId blockId, int index)
    throws BlockLoadingException, BlockNotFoundException {
    final CacheEntry entry = cache.getIfPresent(blockId);
    if (entry == null) {
      throw new BlockNotFoundException();
    } else {
      lru.use(blockId);
      // getData throws BlockLoadingException if load has not completed for the requested chunk
      return entry.getData(index);
    }
  }

  @Override
  public void write(final BlockId blockId,
                    final long offset,
                    final ByteBuffer data,
                    final boolean isLastPacket) throws BlockNotFoundException, BlockNotWritableException, IOException {
    final CacheEntry entry = cache.getIfPresent(blockId);
    if (entry == null) {
      throw new BlockNotFoundException();
      // TODO Simplify more
    } else if (entry.getBlockReceiver() == null) {
      throw new BlockNotWritableException();
    } else {
      final BlockReceiver blockReceiver = entry.getBlockReceiver();
      blockReceiver.writeData(data.array(), offset);

      /*
       * When the packet is the last one of block
       * 1) Mark the block is Complete
       * 2) Notify Memory manager to update memory state
       * 3) Trigger heartbeat to update the metadata immediately
       */
      if (isLastPacket) {
        blockReceiver.completeWrite();
        final long blockSize = blockReceiver.getBlockSize();
        final long nWritten = blockReceiver.getTotalWritten();
        memoryManager.writeSuccess(blockId, blockSize, nWritten, entry.isPinned());
        heartBeatTriggerManager.triggerHeartBeat();
      }
    }
  }

  @Override
  public void load(final BlockLoader loader) throws IOException {
    final CacheEntry entry = new CacheEntry(loader);
    if (insertEntry(entry)) {
      LOG.log(Level.INFO, "Add loading block {0}", loader.getBlockId());
      loadingStage.onNext(loader);
    }
  }

  @Override
  public void prepareToWrite(final BlockReceiver receiver) throws IOException, BlockNotFoundException {
    final CacheEntry entry = new CacheEntry(receiver);
    if (insertEntry(entry)) {
      cacheAdmissionController.reserveSpace(receiver.getBlockId(), receiver.getBlockSize(), receiver.isPinned());
    }
  }

  /**
   * Insert an entry into the cache.
   * The purpose we insert blockLoader before
   * the blockLoader has actual data is to prevent loading duplicate blocks.
   * @param entry an entry to be inserted.
   * @return {@code true} If the entry is inserted successfully.
   * @throws IOException
   */
  private boolean insertEntry(final CacheEntry entry) throws IOException {
    final Callable<CacheEntry> callable = new CacheEntryCaller(entry, memoryManager);
    final CacheEntry returnedEntry;
    try {
      returnedEntry = cache.get(entry.getBlockId(), callable);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
    return entry == returnedEntry;
  }


  @Override
  public int getLoadingBufferSize() {
    return loadingBufferSize;
  }

  @Override
  public void clear() { // TODO: do we need a stage for this as well? For larger caches, it could take awhile
    final List<BlockId> blockIds = lru.evictAll(true); // TODO add CLI options for evicting all except for pinned
    for (final BlockId blockId : blockIds) {
      cache.invalidate(blockId);
    }
    cache.cleanUp();
    memoryManager.clearHistory();
  }

  @Override
  public CacheStatistics getStatistics() {
    return memoryManager.getStatistics();
  }

  @Override
  public CacheUpdates pullUpdates() {
    return memoryManager.pullUpdates();
  }
}
