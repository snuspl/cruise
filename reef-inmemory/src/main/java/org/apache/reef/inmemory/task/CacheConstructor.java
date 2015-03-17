package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Constructs an instance of the Guava Cache.
 *
 * Guava Caches are segmented by key. Guava provides atomic "get-if-absent-compute-and-put" semantics,
 * with the properties:
 * - Compute only locks the segment the key is in.
 *     (The number of segments is decided according to the concurrency level.)
 * - Reads are non-locking for keys that are not computing.
 * - Reads wait when keys are computing, preserving the atomic semantics.
 * In Surf, the "compute" is fast because it simply creates a BlockLoader object. (The actual block
 * loading is done in a separate stage.) Thus, compute and read operations should never block for a significant time.
 *
 * Surf runs its own eviction algorithms, and invalidates keys that are chosen for eviction.
 * Guava Cache triggers a removal notification when keys are invalidated.
 * Surf's MemoryManager is then updated through the RemovalListener.
 */
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
