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
package edu.snu.cay.services.ps.worker.impl;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.snu.cay.services.ps.PSParameters.KeyCodecName;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import edu.snu.cay.services.ps.worker.parameters.*;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.common.Statistics;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Parameter Server worker that interacts with servers.
 * A single instance of this class can be used by more than one thread safely, if and only if
 * the Codec classes are thread-safe.
 *
 * There are a few client-side optimizations that can be configured.
 * A serialized and hashed representation of a key is cached, avoiding these costs.
 * See {@link WorkerKeyCacheSize}.
 * The remaining configurations are related to the worker-side threads.
 * See {@link WorkerThread}.
 */
@EvaluatorSide
public final class AsyncParameterWorker<K, P, V> implements ParameterWorker<K, P, V>, WorkerHandler<K, P, V> {
  private static final Logger LOG = Logger.getLogger(AsyncParameterWorker.class.getName());

  /**
   * The maximum number to resend push/pull requests
   * when {@link NetworkException} occurred while sending requests.
   */
  static final int MAX_RESEND_COUNT = 10;

  /**
   * An interval between the time to resend push/pull requests
   * when {@link NetworkException} occurred while sending requests.
   */
  static final long RESEND_INTERVAL_MS = 100;

  /**
   * The maximum number to restart pull requests from the beginning
   * when the pull reply do not arrive within timeout {@link PullRetryTimeoutMs},
   * or the pull request is rejected by server.
   */
  static final int MAX_PULL_RETRY_COUNT = 10;

  /**
   * Object for processing preValues and applying updates to existing values.
   */
  private final ParameterUpdater<K, P, V> parameterUpdater;

  /**
   * Resolve to a server's Network Connection Service identifier based on hashed key.
   */
  private final ServerResolver serverResolver;

  /**
   * A map of pending pulls, used to reconcile the asynchronous messaging with the synchronous CacheLoader call.
   */
  private final ConcurrentMap<K, PullFuture<V>> pendingPulls;

  /**
   * Number of threads.
   */
  private final int numThreads;

  /**
   * Thread pool, where each thread is submitted.
   */
  private final ExecutorService threadPool;

  /**
   * Running threads.
   */
  private final WorkerThread<K, P, V>[] threads;

  /**
   * A cache that stores encoded (serialized) keys and hashes.
   */
  private final LoadingCache<K, EncodedKey<K>> encodedKeyCache;

  /**
   * Send messages to the server using this field.
   * Without {@link InjectionFuture}, this class creates an injection loop with
   * classes related to Network Connection Service and makes the job crash (detected by Tang).
   */
  private final InjectionFuture<WorkerMsgSender<K, P>> sender;

  private final long logPeriod;
  private final Statistics[] pushStats;
  private final Statistics[] encodeStats;
  private final Statistics[] pullStats;
  private final long[] startTimes;
  private final Ticker ticker = Ticker.systemTicker();

  @Inject
  private AsyncParameterWorker(@Parameter(ParameterWorkerNumThreads.class) final int numThreads,
                               @Parameter(WorkerQueueSize.class) final int queueSize,
                               @Parameter(WorkerExpireTimeout.class) final long cacheExpireTimeout,
                               @Parameter(PullRetryTimeoutMs.class) final long pullRetryTimeoutMs,
                               @Parameter(WorkerKeyCacheSize.class) final int keyCacheSize,
                               @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                               @Parameter(WorkerLogPeriod.class) final long logPeriod,
                               final ParameterUpdater<K, P, V> parameterUpdater,
                               final ServerResolver serverResolver,
                               final InjectionFuture<WorkerMsgSender<K, P>> sender) {
    this.numThreads = numThreads;
    this.parameterUpdater = parameterUpdater;
    this.serverResolver = serverResolver;
    this.sender = sender;
    this.pendingPulls = new ConcurrentHashMap<>();
    this.pullStats = Statistics.newInstances(numThreads);
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.threads = initThreads(queueSize, cacheExpireTimeout, pullRetryTimeoutMs);
    this.encodedKeyCache = CacheBuilder.newBuilder()
        .maximumSize(keyCacheSize)
        .build(new CacheLoader<K, EncodedKey<K>>() {
          @Override
          public EncodedKey<K> load(final K key) throws Exception {
            return new EncodedKey<>(key, keyCodec);
          }
        });

    this.logPeriod = TimeUnit.NANOSECONDS.convert(logPeriod, TimeUnit.MILLISECONDS);
    this.pushStats = Statistics.newInstances(numThreads);
    this.encodeStats = Statistics.newInstances(numThreads);
    this.startTimes = new long[numThreads];
    final long currentTime = ticker.read();
    for (int i = 0; i < numThreads; ++i) {
      startTimes[i] = currentTime;
    }
    LOG.log(Level.INFO, "Parameter worker log period = {0} ms", logPeriod);
  }

  /**
   * Call after initializing threadPool.
   */
  @SuppressWarnings("unchecked")
  private WorkerThread<K, P, V>[] initThreads(final int queueSize,
                                              final long cacheExpireTimeout,
                                              final long pullRetryTimeoutMs) {
    LOG.log(Level.INFO, "Initializing {0} threads", numThreads);
    final WorkerThread<K, P, V>[] initialized = new WorkerThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      initialized[i] = new WorkerThread<>(pendingPulls, serverResolver, sender, queueSize,
          cacheExpireTimeout, pullRetryTimeoutMs, pullStats[i]);
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

  void push(final EncodedKey<K> encodedKey, final P preValue) {
    final int partitionId = getPartitionIndex(encodedKey.getHash());
    final int threadId = partitionId % numThreads;
    threads[threadId].enqueue(new PushOp(encodedKey, preValue, threadId));
  }

  @Override
  public V pull(final K key) {
    try {
      return pull(encodedKeyCache.get(key));
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  V pull(final EncodedKey<K> encodedKey) {
    final PullOp pullOp = new PullOp(encodedKey);
    final int partitionId = getPartitionIndex(encodedKey.getHash());
    final int threadId = partitionId % numThreads;
    threads[threadId].enqueue(pullOp);
    return pullOp.get();
  }

  @Override
  public List<V> pull(final List<K> keys) {
    // transform keys to encoded keys
    final List<EncodedKey<K>> encodedKeys = new ArrayList<>(keys.size());
    for (final K key : keys) {
      try {
        encodedKeys.add(encodedKeyCache.get(key));
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    return pullEncodedKeys(encodedKeys);
  }

  private List<V> pullEncodedKeys(final List<EncodedKey<K>> encodedKeys) {
    final List<PullOp> pullOps = new ArrayList<>(encodedKeys.size());
    for (final EncodedKey<K> encodedKey : encodedKeys) {
      final PullOp pullOp = new PullOp(encodedKey);
      pullOps.add(pullOp);
      final int partitionId = getPartitionIndex(encodedKey.getHash());
      final int threadId = partitionId % numThreads;
      threads[threadId].enqueue(pullOp);
    }
    final List<V> values = new ArrayList<>(pullOps.size());
    for (final PullOp pullOp : pullOps) {
      values.add(pullOp.get());
    }
    return values;
  }

  public void invalidateAll() {
    for (int i = 0; i < numThreads; i++) {
      threads[i].invalidateAll();
    }
  }

  private int getPartitionIndex(final int keyHash) {
    return keyHash % numThreads;
  }

  /**
   * Close the worker, after waiting a maximum of {@code timeoutMs} milliseconds
   * for queued messages to be sent.
   */
  @Override
  public void close(final long timeoutMs) throws InterruptedException, TimeoutException, ExecutionException {

    final Future result = Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        // Close all threads
        for (int i = 0; i < numThreads; i++) {
          threads[i].startClose();
        }
        // Wait for close to complete on all threads
        for (int i = 0; i < numThreads; i++) {
          threads[i].waitForClose();
        }
      }
    });

    result.get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Handles incoming pull replies, by setting the value of the future.
   * This will notify the WorkerThread's (synchronous) CacheLoader method to continue.
   */
  @Override
  public void processPullReply(final K key, final V value) {
    final PullFuture<V> future = pendingPulls.get(key);
    if (future != null) {
      future.setValue(value);

    } else {
      // Because we assign each key to a dedicated thread, there can be at most one active pendingPull for a key.
      // But occasionally, multiple responses for a single pendingPull may arrive
      // if the worker retried due to the late response from the target server.
      LOG.log(Level.WARNING, "Pending pull was not found for key {0}." +
          " Response for the key may have arrived earlier from another server", key);
    }
  }
  /**
   * Handles incoming pull rejects, by rejecting the future.
   * This will notify the WorkerThread's (synchronous) CacheLoader method to retry.
   */
  @Override
  public void processPullReject(final K key) {
    final PullFuture<V> future = pendingPulls.get(key);
    if (future != null) {
      LOG.log(Level.INFO, "Pull operation for key {0} is rejected." +
          " It means that the corresponding server is closing or already closed.", key);
      future.reject();

    } else {
      // Because we assign each key to a dedicated thread, there can be at most one active pendingPull for a key.
      // But occasionally, multiple responses for a single pendingPull may arrive
      // if the worker retried due to the late response from the target server.
      LOG.log(Level.WARNING, "Pending pull was not found for key {0}." +
          " Response for the key may have arrived earlier from another server", key);
    }
  }

  /**
   * Handles incoming push rejects, by retrying push request.
   * This function has been added to this class that it implements {@link WorkerHandler} interface.
   */
  @Override
  public void processPushReject(final K key, final P preValue) {
    this.push(key, preValue);
  }

  /**
   * A simple Future that will wait on a get, until a value is set.
   * We do not implement a true Future, because this is simpler.
   */
  private static final class PullFuture<V> {
    private boolean rejected = false;
    private V value = null;

    /**
     * Block until a value is set or the maximum waiting time elapses.
     * @param timeout the maximum time to wait in milliseconds
     * @return the value, or null when it fails to get the value in given timeout
     */
    synchronized V getValue(final long timeout) {
      if (value == null && !rejected) {
        try {
          wait(timeout);
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
    synchronized void setValue(final V value) {
      this.value = value;
      notify();
    }

    /**
     * Wake up the waiting thread without setting a value, in order to retry.
     */
    synchronized void reject() {
      rejected = true;
      notify();
    }

    /**
     * Reset pull future for the next retry.
     * The rejected field should be reset, because it's valid only for current try.
     * We don't need to reset the value field, because the value obtained from
     * the previous request is also valid for the next retries.
     * It may happen if the response from the previous request arrives
     * after cleanup the current try and before the response from the next request.
     */
    synchronized void reset() {
      rejected = false;
    }
  }

  private void printStatistics(final int threadId, final long elapsedTime) {
    final Statistics pushStat = pushStats[threadId];
    final Statistics encodeStat = encodeStats[threadId];
    final Statistics pullStat = pullStats[threadId];
    LOG.log(Level.INFO, "PS Elapsed Time: {0}, PS Worker Thread Id: {1}, PS Worker Push Avg: {2}, " +
            "PS Worker Push Sum: {3}, PS Worker Push Count:{4}, PS Worker Encode Avg: {5}, " +
            "PS Worker Encode Sum: {6}, PS Worker Pull Avg: {7}, PS Worker Pull Sum: {8}, PS Worker Pull Count: {9}",
        new Object[]{elapsedTime / 1e9D, threadId, String.format("%g", pushStat.avg()),
            String.format("%g", pushStat.sum()), pushStat.count(), String.format("%g", encodeStat.avg()),
            String.format("%g", encodeStat.sum()), String.format("%g", pullStat.avg()),
            String.format("%g", pullStat.sum()), String.format("%g", pullStat.count())});
    startTimes[threadId] = ticker.read();
    pushStat.reset();
    encodeStat.reset();
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

    Wrapped(final V value) {
      this.value = value;
    }

    V getValue() {
      return value;
    }

    void setValue(final V value) {
      this.value = value;
    }
  }

  /**
   * A push operation.
   */
  private class PushOp implements Op<K, V> {
    private final EncodedKey<K> encodedKey;
    private final P preValue;
    private final int threadId;

    PushOp(final EncodedKey<K> encodedKey, final P preValue, final int threadId) {
      this.encodedKey = encodedKey;
      this.preValue = preValue;
      this.threadId = threadId;
    }

    /**
     * First, update the local value, only if it is already cached.
     * Second, send the update to the remote PS.
     * @param kvCache the raw LoadingCache, provided by the Partition.
     */
    @Override
    public void apply(final LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache) {
      final long pushStartTime = ticker.read();
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
      int resendCount = 0;
      while (true) {
        if (resendCount++ > MAX_RESEND_COUNT) {
          throw new RuntimeException("Fail to send a push message");
        }

        // re-resolve server for every retry
        // since an operation may throw NetworkException when routing table is obsolete
        final String serverId = serverResolver.resolveServer(encodedKey.getHash());
        LOG.log(Level.FINEST, "Resolve server for encodedKey. key: {0}, hash: {1}, serverId: {2}",
            new Object[]{encodedKey.getKey(), encodedKey.getHash(), serverId});

        try {
          final long encodeStartTime = ticker.read();
          sender.get().sendPushMsg(serverId, encodedKey, preValue);

          // Can reach here only when sendPushMsg succeeds.
          final long endTime = ticker.read();
          pushStats[threadId].put(endTime - pushStartTime);
          encodeStats[threadId].put(endTime - encodeStartTime);
          final long elapsedTime = endTime - startTimes[threadId];
          if (logPeriod > 0 && elapsedTime > logPeriod) {
            printStatistics(threadId, elapsedTime);
          }
          break;
        } catch (final NetworkException e) {
          LOG.log(Level.FINE, "NetworkException while sending push msg. Do retry", e);
        }

        LOG.log(Level.FINE, "Wait {0} ms before resending a push msg", RESEND_INTERVAL_MS);
        try {
          Thread.sleep(RESEND_INTERVAL_MS);
        } catch (final InterruptedException e) {
          LOG.log(Level.WARNING, "Interrupted while waiting for routing table to be updated", e);
        }
      }
    }
  }

  /**
   * A pull operation.
   * Also exposes a blocking {@link #get} method to retrieve the result of the pull.
   */
  private class PullOp implements Op<K, V> {
    private final EncodedKey<K> encodedKey;
    private V value;

    PullOp(final EncodedKey<K> encodedKey) {
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
   * {@link edu.snu.cay.services.ps.server.api.ParameterServer}.
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
  private static class WorkerThread<K, P, V> implements Runnable {
    private static final long QUEUE_TIMEOUT_MS = 3000;
    private static final String STATE_RUNNING = "RUNNING";
    private static final String STATE_CLOSING = "CLOSING";
    private static final String STATE_CLOSED = "CLOSED";

    private final LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache;
    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.
    private final Ticker ticker = Ticker.systemTicker();

    private final StateMachine stateMachine;

    WorkerThread(final ConcurrentMap<K, PullFuture<V>> pendingPulls,
                 final ServerResolver serverResolver,
                 final InjectionFuture<WorkerMsgSender<K, P>> sender,
                 final int queueSize,
                 final long cacheExpireTimeout,
                 final long pullRetryTimeoutMs,
                 final Statistics pullStat) {
      this.kvCache = initCache(pendingPulls, serverResolver, sender, cacheExpireTimeout, pullRetryTimeoutMs, pullStat);
      this.queue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize / 10;
      this.localOps = new ArrayList<>(drainSize);
      this.stateMachine = initStateMachine();
    }

    private LoadingCache<EncodedKey<K>, Wrapped<V>> initCache(final ConcurrentMap<K, PullFuture<V>> pendingPulls,
                                                              final ServerResolver serverResolver,
                                                              final InjectionFuture<WorkerMsgSender<K, P>> sender,
                                                              final long cacheExpireTimeout,
                                                              final long pullRetryTimeoutMs,
                                                              final Statistics pullStat) {
      return CacheBuilder.newBuilder()
          .concurrencyLevel(1)
          .expireAfterWrite(cacheExpireTimeout, TimeUnit.MILLISECONDS)
          .build(new CacheLoader<EncodedKey<K>, Wrapped<V>>() {

            @Override
            public Wrapped<V> load(final EncodedKey<K> encodedKey) {
              final PullFuture<V> future = new PullFuture<>();
              pendingPulls.put(encodedKey.getKey(), future);

              V value;

              int retryCount = 0;
              while (true) {
                if (retryCount++ > MAX_PULL_RETRY_COUNT) {
                  throw new RuntimeException("Fail to load a value for pull");
                }

                // 1. try sending msg to server
                sendPullMsg(encodedKey);

                // 2. wait the result from the server.
                //
                // PullFuture returns null,
                // 1) when the msg is rejected by server,
                // 2) or when the server does not respond within RETRY_INTERVAL_MS
                // The case 2) can be divided into three reasons:
                // 2A) the minimum processing time for pull is longer than RETRY_INTERVAL_MS
                // 2B) the server is overloaded
                // 2C) the msg is missing due to network or other problems
                // we should adjust timeout to be large enough to avoid 2A and not to worsen 2B,
                // but small enough to quickly recover from 2C.
                value = future.getValue(pullRetryTimeoutMs);

                if (value != null) {
                  break;
                } else {
                  future.reset();
                }
              }

              pendingPulls.remove(encodedKey.getKey());
              return new Wrapped<>(value);
            }

            /**
             * Sends a pull msg for the {@code encodedKey} to the target server.
             * @param encodedKey encoded key
             */
            private void sendPullMsg(final EncodedKey<K> encodedKey) {
              int resendCount = 0;
              while (true) {
                if (resendCount++ > MAX_RESEND_COUNT) {
                  throw new RuntimeException("Fail to send a pull msg");
                }

                // Re-resolve server for every retry, because msg sender throws NetworkException
                // when routing table is obsolete and indicates non-existing server.
                final String serverId = serverResolver.resolveServer(encodedKey.getHash());
                LOG.log(Level.FINEST, "Resolve server for encodedKey. key: {0}, hash: {1}, serverId: {2}",
                    new Object[]{encodedKey.getKey(), encodedKey.getHash(), serverId});

                try {
                  final long beginTick = ticker.read();
                  sender.get().sendPullMsg(serverId, encodedKey);
                  pullStat.put(ticker.read() - beginTick);
                  break;
                } catch (final NetworkException e) {
                  LOG.log(Level.FINE, "NetworkException while sending pull msg. Do retry", e);
                }

                LOG.log(Level.FINE, "Wait {0} ms before resending a pull msg", RESEND_INTERVAL_MS);
                try {
                  Thread.sleep(RESEND_INTERVAL_MS);
                } catch (final InterruptedException e) {
                  LOG.log(Level.WARNING, "Interrupted while waiting for routing table to be updated", e);
                }
              }
            }
          });
    }

    private StateMachine initStateMachine() {
      return StateMachine.newBuilder()
          .addState(STATE_RUNNING, "Server thread is running. It executes operations in the queue.")
          .addState(STATE_CLOSING, "Server thread is closing. It will be closed after processing whole remaining ops.")
          .addState(STATE_CLOSED, "Server thread is closed. It finished processing whole remaining operations.")
          .addTransition(STATE_RUNNING, STATE_CLOSING, "Time to close the thread.")
          .addTransition(STATE_CLOSING, STATE_CLOSED, "Closing the thread is done.")
          .setInitialState(STATE_RUNNING)
          .build();
    }

    /**
     * Enqueue an operation onto the queue, blocking if the queue is full.
     * When the queue is full, this method will block; thus, a full queue will block the thread calling
     * enqueue, e.g., from the NCS message thread pool, until the queue is drained.
     *
     * @param op the operation to enqueue
     */
    void enqueue(final Op<K, V> op) {
      try {
        queue.put(op);
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
      }
    }

    /**
     * Invalidate all cached pulls.
     */
    void invalidateAll() {
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
      try {
        while (stateMachine.getCurrentState().equals(STATE_RUNNING) || !queue.isEmpty()) {
          // First, poll and apply. The timeout allows the run thread to close cleanly within timeout ms.
          try {
            final Op<K, V> op = queue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (op == null) {
              continue;
            }
            op.apply(kvCache);
          } catch (final InterruptedException e) {
            LOG.log(Level.WARNING, "Poll failed with InterruptedException", e);
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

        finishClose();
      } catch (final RuntimeException e) {
        LOG.log(Level.SEVERE, "PS worker thread has been down due to RuntimeException", e);
        throw e;
      }
    }

    /**
     * Start closing the thread.
     * The thread will be closed after processing for all pending operations.
     */
    void startClose() {
      stateMachine.setState(STATE_CLOSING);
    }

    /**
     * Notify that the thread is closed successfully.
     * It wakes up threads waiting in {@link #waitForClose()}.
     */
    private synchronized void finishClose() {
      stateMachine.setState(STATE_CLOSED);
      notifyAll();
    }

    /**
     * Wait until thread is closed successfully.
     */
    synchronized void waitForClose() {
      while (!stateMachine.getCurrentState().equals(STATE_CLOSED)) {
        try {
          wait();
        } catch (final InterruptedException e) {
          LOG.log(Level.WARNING, "InterruptedException while waiting for close to complete", e);
        }
      }
    }
  }
}
