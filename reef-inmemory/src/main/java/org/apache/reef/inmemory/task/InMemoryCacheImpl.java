package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.wake.EStage;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
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
  private final CacheEntryFactory cacheEntryFactory;

  @Inject
  public InMemoryCacheImpl(final Cache<BlockId, CacheEntry> cache,
                           final MemoryManager memoryManager,
                           final CacheAdmissionController cacheAdmissionController,
                           final LRUEvictionManager lru,
                           final EStage<BlockLoader> loadingStage,
                           final @Parameter(CacheParameters.NumServerThreads.class) int numThreads,
                           final @Parameter(CacheParameters.LoadingBufferSize.class) int loadingBufferSize,
                           final HeartBeatTriggerManager heartBeatTriggerManager,
                           final CacheEntryFactory cacheEntryFactory) {
    this.cache = cache;
    this.memoryManager = memoryManager;
    this.cacheAdmissionController = cacheAdmissionController;
    this.lru = lru;
    this.loadingStage = loadingStage;
    this.loadingBufferSize = loadingBufferSize;
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.cacheEntryFactory = cacheEntryFactory;
  }

  @Override
  public byte[] get(final BlockId blockId, int index)
          throws BlockLoadingException, BlockNotFoundException, BlockWritingException {
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
    } else {
      final long nWritten = entry.writeData(data.array(), offset, isLastPacket);

      if (isLastPacket) {
        memoryManager.writeSuccess(blockId, entry.getBlockSize(), entry.isPinned(), nWritten);
        heartBeatTriggerManager.triggerHeartBeat(); // To update the file's metadata immediately
      }
    }
  }

  @Override
  public void load(final BlockLoader loader) throws IOException {
    final CacheEntry entry = cacheEntryFactory.createEntry(loader);
    if (insertEntry(entry)) {
      LOG.log(Level.INFO, "Add loading block {0}", loader.getBlockId());
      loadingStage.onNext(loader);
    }
  }

  @Override
  public void prepareToWrite(final BlockWriter blockWriter) throws IOException {
    final BlockId blockId = blockWriter.getBlockId();
    final long blockSize = blockWriter.getBlockSize();
    final boolean pin = blockWriter.isPinned();

    final CacheEntry entry = cacheEntryFactory.createEntry(blockWriter);

    // Reserve enough memory space in the cache for block
    if (insertEntry(entry)) {
      try {
        cacheAdmissionController.reserveSpace(blockId, blockSize, pin);
      } catch (BlockNotFoundException e) {
        LOG.log(Level.INFO, "Already removed block {0}", blockId);
        throw new IOException(e);
      } catch (MemoryLimitException e) {
        LOG.log(Level.SEVERE, "Memory limit reached", e);
        memoryManager.copyStartFail(blockId, blockSize, pin, e);
        throw new IOException(e);
      }
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

  @Override
  public void delete(final BlockId blockId) {
    final CacheEntry entry = cache.getIfPresent(blockId);
    if (entry == null) {
      LOG.log(Level.INFO, "The entry to delete is not found. BlockId: {0}", blockId);
    } else {
      entry.markAsDeleted();
      cache.invalidate(blockId);
    }
  }
}
