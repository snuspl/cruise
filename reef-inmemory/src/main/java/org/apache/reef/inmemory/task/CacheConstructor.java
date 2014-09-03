package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class CacheConstructor implements ExternalConstructor<Cache> {

  private Logger LOG = Logger.getLogger(CacheConstructor.class.getName());

  private MemoryManager memoryManager;
  private final int numThreads;

  /**
   * Update statistics on cache removal
   */
  private final RemovalListener<BlockId, BlockLoader> removalListener = new RemovalListener<BlockId, BlockLoader>() {
    @Override
    public void onRemoval(RemovalNotification<BlockId, BlockLoader> notification) {
      LOG.log(Level.INFO, "Removed: "+notification.getKey());
      final BlockId blockId = notification.getKey();
      final boolean pinned = notification.getValue().isPinned();
      memoryManager.remove(blockId, pinned);
    }
  };

  @Inject
  public CacheConstructor(final MemoryManager memoryManager,
                          final @Parameter(CacheParameters.NumServerThreads.class) int numThreads) {
    this.memoryManager = memoryManager;
    this.numThreads = numThreads;
  }

  @Override
  public Cache newInstance() {
    return CacheBuilder.newBuilder()
            .removalListener(removalListener)
            .concurrencyLevel(numThreads)
            .build();
  }
}
