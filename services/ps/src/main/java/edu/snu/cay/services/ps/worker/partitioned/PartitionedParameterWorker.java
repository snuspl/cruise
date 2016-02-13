/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.ps.worker.partitioned;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.snu.cay.services.ps.ParameterServerParameters.KeyCodecName;
import edu.snu.cay.services.ps.driver.impl.ServerId;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerExpireTimeout;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerKeyCacheSize;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerNumPartitions;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerQueueSize;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Partitioned Parameter Server worker that interacts with the Partitioned single-node server.
 * A single instance of this class can be used by more than one thread safely, if and only if
 * the Codec classes are thread-safe.
 *
 * There are a few client-side optimizations that can be configured.
 * A serialized and hashed representation of a key is cached, avoiding these costs.
 * See {@link WorkerKeyCacheSize}.
 * The remaining configurations are related to the worker-side partitions.
 * See {@link Partition}.
 */
@EvaluatorSide
public final class PartitionedParameterWorker<K, P, V> implements ParameterWorker<K, P, V> {
  private static final Logger LOG = Logger.getLogger(PartitionedParameterWorker.class.getName());

  /**
   * Network Connection Service identifier of the server.
   */
  private final String serverId;

  /**
   * Object for processing preValues and applying updates to existing values.
   */
  private final ParameterUpdater<K, P, V> parameterUpdater;

  /**
   * A map of pending pulls, used to reconcile the asynchronous messaging with the synchronous CacheLoader call.
   */
  private final ConcurrentMap<K, PullFuture<V>> pendingPulls;

  /**
   * Number of partitions.
   */
  private final int numPartitions;

  /**
   * Max size of each partition's queue.
   */
  private final int queueSize;

  /**
   * Duration in ms to keep local entries cached, after which the entries are expired.
   */
  private final long expireTimeout;

  /**
   * Thread pool, where each Partition is submitted.
   */
  private final ExecutorService threadPool;

  /**
   * Running partitions.
   */
  private final Partition<K, P, V>[] partitions;

  /**
   * A cache that stores encoded (serialized) keys and hashes.
   */
  private final LoadingCache<K, EncodedKey<K>> encodedKeyCache;

  /**
   * Send messages to the server using this field.
   * Without {@link InjectionFuture}, this class creates an injection loop with
   * classes related to Network Connection Service and makes the job crash (detected by Tang).
   */
  private final InjectionFuture<PartitionedWorkerMsgSender<K, P>> sender;

  @Inject
  private PartitionedParameterWorker(@Parameter(ServerId.class) final String serverId,
                                     @Parameter(WorkerNumPartitions.class) final int numPartitions,
                                     @Parameter(WorkerQueueSize.class) final int queueSize,
                                     @Parameter(WorkerExpireTimeout.class) final long expireTimeout,
                                     @Parameter(WorkerKeyCacheSize.class) final int keyCacheSize,
                                     @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                                     final ParameterUpdater<K, P, V> parameterUpdater,
                                     final InjectionFuture<PartitionedWorkerMsgSender<K, P>> sender) {
    this.serverId = serverId;
    this.numPartitions = numPartitions;
    this.queueSize = queueSize;
    this.expireTimeout = expireTimeout;
    this.parameterUpdater = parameterUpdater;
    this.sender = sender;
    this.pendingPulls = new ConcurrentHashMap<>();
    this.threadPool = Executors.newFixedThreadPool(numPartitions);
    this.partitions = initPartitions();
    this.encodedKeyCache = CacheBuilder.newBuilder()
        .maximumSize(keyCacheSize)
        .build(new CacheLoader<K, EncodedKey<K>>() {
          @Override
          public EncodedKey<K> load(final K key) throws Exception {
            return new EncodedKey<>(key, keyCodec);
          }
        });
  }

  /**
   * Call after initializing numPartitions and threadPool.
   */
  @SuppressWarnings("unchecked")
  private Partition<K, P, V>[] initPartitions() {
    LOG.log(Level.INFO, "Initializing {0} partitions", numPartitions);
    final Partition<K, P, V>[] initialized = new Partition[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      initialized[i] = new Partition<>(pendingPulls, sender, serverId, queueSize, expireTimeout);
      threadPool.submit(initialized[i]);
    }
    return initialized;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void push(final K key, final P preValue) {
    try {
      push(encodedKeyCache.get(key), preValue);
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void push(final EncodedKey<K> encodedKey, final P preValue) {
    partitions[getPartitionIndex(encodedKey.getHash())].enqueue(new PushOp(encodedKey, preValue));
  }

  @Override
  public V pull(final K key) {
    try {
      return pull(encodedKeyCache.get(key));
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public V pull(final EncodedKey<K> encodedKey) {
    final PullOp pullOp = new PullOp(encodedKey);
    partitions[getPartitionIndex(encodedKey.getHash())].enqueue(pullOp);
    return pullOp.get();
  }

  public void invalidateAll() {
    for (int i = 0; i < numPartitions; i++) {
      partitions[i].invalidateAll();
    }
  }

  private int getPartitionIndex(final int keyHash) {
    return keyHash % numPartitions;
  }

  /**
   * Close the worker, after waiting for queued messages to be sent.
   */
  public void close() {
    // Close all partitions
    for (int i = 0; i < numPartitions; i++) {
      partitions[i].close();
    }
    // Wait for shutdown to complete on all partitions
    for (int i = 0; i < numPartitions; i++) {
      partitions[i].waitForShutdown();
    }
  }

  /**
   * Handles incoming pull replies, by setting the value of the future.
   * This will notify the Partition's (synchronous) CacheLoader method to continue.
   * Called by {@link PartitionedWorkerHandler#processReply}.
   */
  public void processReply(final K key, final V value) {
    final PullFuture<V> future = pendingPulls.get(key);
    if (future != null) {
      future.setValue(value);
    } else {
      // Because we use partitions, there can be at most one active pendingPull for a key.
      // Thus, a null value should never appear.
      throw new RuntimeException(String.format("Pending pull was not found for key %s", key));
    }
  }

  /**
   * A simple Future that will wait on a get, until a value is set.
   * We do not implement a true Future, because this is simpler.
   */
  private static final class PullFuture<V> {
    private V value;

    /**
     * Block until a value is set.
     * @return the value
     */
    public synchronized V getValue() {
      while (value == null) {
        try {
          wait();
        } catch (final InterruptedException e) {
          LOG.log(Level.WARNING, "InterruptedException on wait", e);
        }
      }
      return value;
    }

    /**
     * Set the value and unblock all waiting gets.
     * @param value the value
     */
    public synchronized void setValue(final V value) {
      this.value = value;
      notify();
    }
  }

  /**
   * A generic operation; operations are queued at each Partition.
   */
  private interface Op<K, V> {
    /**
     * Method to apply when dequeued by the Partition.
     * @param kvCache the raw LoadingCache, provided by the Partition.
     */
    void apply(LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache);
  }

  /**
   * Wrapped values for use within each partition's cache.
   * Wrapping allows the partition to replace the value on a local update,
   * without updating the write time of the cache entry.
  */
  private static class Wrapped<V> {
    private V value;

    public Wrapped(final V value) {
      this.value = value;
    }

    public V getValue() {
      return value;
    }

    public void setValue(final V value) {
      this.value = value;
    }
  }

  /**
   * A push operation.
   */
  private class PushOp implements Op<K, V> {
    private final EncodedKey<K> encodedKey;
    private final P preValue;

    public PushOp(final EncodedKey<K> encodedKey, final P preValue) {
      this.encodedKey = encodedKey;
      this.preValue = preValue;
    }

    /**
     * First, update the local value, only if it is already cached.
     * Second, send the update to the remote PS.
     * @param kvCache the raw LoadingCache, provided by the Partition.
     */
    @Override
    public void apply(final LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache) {
      final Wrapped<V> wrapped = kvCache.getIfPresent(encodedKey);

      // If it exists, update the local value, without updating the cache's write time
      if (wrapped != null) {
        final V oldValue = wrapped.getValue();
        final V deltaValue = parameterUpdater.process(encodedKey.getKey(), preValue);
        if (deltaValue == null) {
          return;
        }
        final V updatedValue = parameterUpdater.update(oldValue, deltaValue);
        wrapped.setValue(updatedValue);
      }

      // Send to remote PS
      sender.get().sendPushMsg(serverId, encodedKey, preValue);
    }
  }

  /**
   * A pull operation.
   * Also exposes a blocking {@link #get} method to retrieve the result of the pull.
   */
  private class PullOp implements Op<K, V> {
    private final EncodedKey<K> encodedKey;
    private V value;

    public PullOp(final EncodedKey<K> encodedKey) {
      this.encodedKey = encodedKey;
    }

    /**
     * Delegate loading to the cache, then update the value and notify waiting gets.
     * @param kvCache the raw LoadingCache, provided by the Partition.
     */
    @Override
    public void apply(final LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache) {
      try {
        final V loadedValue = kvCache.get(encodedKey).getValue();
        synchronized (this) {
          this.value = loadedValue;
          notify();
        }
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * A blocking get.
     * @return the value
     */
    public V get() {
      synchronized (this) {
        while (value == null) {
          try {
            wait();
          } catch (final InterruptedException e) {
            LOG.log(Level.WARNING, "InterruptedException on wait", e);
          }
        }
        return value;
      }
    }
  }

  /**
   * A partition for the cache on the Worker.
   * The basic structure is similar to the partition for the Server at
   * {@link edu.snu.cay.services.ps.server.partitioned.PartitionedParameterServer}.
   *
   * The partitions at the Worker can be independent of the partitions at the Server. In other words,
   * the number of worker-side partitions does not have to be equal to the number of server-side partitions.
   *
   * A remotely read pull remains in the local cache for a duration of expireTimeout.
   * Pushes are applied locally while the parameter is cached.
   * The single queue-and-thread, combined with the server, provides a guarantee that
   * all previous local pushes are applied to a pull, if it is locally cached.
   *
   * This means pull operations are queued behind push operations.
   * We should further explore this trade-off with real ML workloads.
   */
  private static class Partition<K, P, V> implements Runnable {
    private static final long QUEUE_TIMEOUT_MS = 3000;

    private final LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache;
    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.

    private volatile boolean close = false;
    private volatile boolean shutdown = false;

    public Partition(final ConcurrentMap<K, PullFuture<V>> pendingPulls,
                     final InjectionFuture<PartitionedWorkerMsgSender<K, P>> sender,
                     final String serverId,
                     final int queueSize,
                     final long expireTimeout) {
      kvCache = CacheBuilder.newBuilder()
          .concurrencyLevel(1)
          .expireAfterWrite(expireTimeout, TimeUnit.MILLISECONDS)
          .build(new CacheLoader<EncodedKey<K>, Wrapped<V>>() {
            @Override
            public Wrapped<V> load(final EncodedKey<K> encodedKey) throws Exception {
              final PullFuture<V> future = new PullFuture<>();
              pendingPulls.put(encodedKey.getKey(), future);
              sender.get().sendPullMsg(serverId, encodedKey);
              final V value = future.getValue();
              pendingPulls.remove(encodedKey.getKey());
              return new Wrapped<>(value);
            }
          });
      queue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize / 10;
      this.localOps = new ArrayList<>(drainSize);
    }

    /**
     * Enqueue an operation onto the queue, blocking if the queue is full.
     * When the queue is full, this method will block; thus, a full queue will block the thread calling
     * enqueue, e.g., from the NCS message thread pool, until the queue is drained.
     *
     * @param op the operation to enqueue
     */
    public void enqueue(final Op<K, V> op) {
      try {
        queue.put(op);
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
        return;
      }
    }

    /**
     * Invalidate all cached pulls.
     */
    public void invalidateAll() {
      kvCache.invalidateAll();
    }


    /**
     * @return number of pending operations in the queue.
     */
    public int opsPending() {
      int opsPending = 0;
      opsPending += queue.size();
      opsPending += localOps.size();
      return opsPending;
    }

    /**
     * Loop that dequeues operations and applies them.
     * Dequeues are only performed through this thread.
     */
    @Override
    public void run() {
      while (!close || !queue.isEmpty()) {
        // First, poll and apply. The timeout allows the run thread to close cleanly within timeout ms.
        try {
          final Op<K, V> op = queue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (op == null) {
            continue;
          }
          op.apply(kvCache);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
          continue;
        }

        // Then, drain up to drainSize of the remaining queue and apply.
        // Calling drainTo does not block if queue is empty, which is why we poll first.
        // This should be faster than polling each op, because the blocking queue's lock is only acquired once.
        queue.drainTo(localOps, drainSize);
        for (final Op<K, V> op : localOps) {
          op.apply(kvCache);
        }
        localOps.clear();
      }
      shutdown();
    }

    /**
     * Cleanly close the run thread.
     */
    public void close() {
      close = true;
    }

    private synchronized void shutdown() {
      shutdown = true;
      notifyAll();
    }

    /**
     * Wait for shutdown confirmation (clean close has finished).
     */
    public synchronized void waitForShutdown() {
      while (!shutdown) {
        try {
          wait();
        } catch (final InterruptedException e) {
          LOG.log(Level.WARNING, "InterruptedException while waiting for close to complete", e);
        }
      }
    }
  }
}
