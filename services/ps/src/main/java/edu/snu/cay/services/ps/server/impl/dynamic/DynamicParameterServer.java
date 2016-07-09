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
package edu.snu.cay.services.ps.server.impl.dynamic;

import com.google.common.base.Ticker;
import edu.snu.cay.common.metric.InsertableMetricTracker;
import edu.snu.cay.common.metric.MetricException;
import edu.snu.cay.common.metric.MetricsCollector;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.common.Statistics;
import edu.snu.cay.services.ps.metric.ConstantsForServer;
import edu.snu.cay.services.ps.metric.ServerMetricsMsgSender;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.services.ps.server.api.ServerSideReplySender;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.parameters.ServerLogPeriod;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of Parameter Server, whose partitions can dynamically move in and out.
 * The parameters are stored in MemoryStore, in most cases the local MemoryStore in the same Evaluator.
 * If {@link edu.snu.cay.services.ps.common.resolver.DynamicServerResolver} has not reflected the
 * up-to-date result of data migration, then PS will receive the requests for the block which has moved out to
 * another MemoryStore. Even in such case, the EM guarantees to redirect the request to the MemoryStore that
 * currently has the data block.
 *
 * Other parts in this implementation workS almost same as {@code StaticParameterServer}.
 */
public final class DynamicParameterServer<K, P, V> implements ParameterServer<K, P, V> {
  private static final Logger LOG = Logger.getLogger(DynamicParameterServer.class.getName());

  /**
   * Initial delay before sending the first metric.
   */
  private static final long METRIC_INIT_DELAY_MS = 3000;

  /**
   * Max size of each thread's queue.
   */
  private final int queueSize;

  /**
   * Thread pool, where each Partition is submitted.
   */
  private final ExecutorService threadPool;

  /**
   * Running threads. A thread can process operations of more than one partition.
   */
  private final Map<Integer, ServerThread<K, V>> threads;

  /**
   * Object for processing preValues and applying updates to existing values.
   */
  private final ParameterUpdater<K, P, V> parameterUpdater;

  /**
   * Sender that sends pull responses.
   */
  private final ServerSideReplySender<K, V> sender;

  /**
   * MemoryStore instance to access the data.
   */
  private final MemoryStore<HashedKey<K>> memoryStore;

  /**
   * Object for resolving the block id assigned to given data.
   */
  private final BlockResolver<HashedKey<K>> blockResolver;

  /**
   * Object for assigning a thread per block.
   */
  private final ThreadResolver threadResolver;

  /**
   * Time period to log the server's statistics.
   */
  private final long logPeriod;

  /**
   * Statistics of the processing time of push operation.
   */
  private final Statistics[] pushStats;

  /**
   * Statistics of the processing time of pull operation.
   */
  private final Statistics[] pullStats;

  /**
   * Statistics of the processing time of both operations - push and pull.
   */
  private final Statistics[] requestStats;

  /**
   * Statistics of the waiting time of push operation since enqueued.
   */
  private final Statistics[] pushWaitStats;

  /**
   * Statistics of the waiting time of push operation.
   */
  private final Statistics[] pullWaitStats;

  /**
   * Bookkeeping start time of the processing threads.
   */
  private long[] startTimes;

  /**
   * Ticker to track the time.
   */
  private final Ticker ticker = Ticker.systemTicker();

  /**
   * Collects metrics within each window.
   */
  private final MetricsCollector metricsCollector;

  /**
   * Records user-defined Metrics (e.g., push-time, pull-time).
   */
  private final InsertableMetricTracker insertableMetricTracker;

  /**
   * Builds and sends ServerMetricsMessages by using received Metrics.
   */
  private final ServerMetricsMsgSender serverMetricsMsgSender;

  /**
   * The discrete time unit to send metrics.
   */
  private int window;

  @Inject
  private DynamicParameterServer(final MemoryStore<HashedKey<K>> memoryStore,
                                 final BlockResolver<HashedKey<K>> blockResolver,
                                 @Parameter(ServerNumThreads.class) final int numThreads,
                                 @Parameter(ServerQueueSize.class) final int queueSize,
                                 @Parameter(ServerLogPeriod.class) final int logPeriod,
                                 final MetricsCollector metricsCollector,
                                 final InsertableMetricTracker insertableMetricTracker,
                                 final ServerMetricsMsgSender serverMetricsMsgSender,
                                 final ParameterUpdater<K, P, V> parameterUpdater,
                                 final ServerSideReplySender<K, V> sender) {
    this.memoryStore = memoryStore;
    this.blockResolver = blockResolver;
    this.queueSize = queueSize;
    this.logPeriod = TimeUnit.NANOSECONDS.convert(logPeriod, TimeUnit.MILLISECONDS);
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.threads = initThreads(numThreads);
    this.parameterUpdater = parameterUpdater;
    this.sender = sender;
    this.threadResolver = new ThreadResolver(numThreads);
    this.pushStats = Statistics.newInstances(numThreads);
    this.pullStats = Statistics.newInstances(numThreads);
    this.requestStats = Statistics.newInstances(numThreads);
    this.pushWaitStats = Statistics.newInstances(numThreads);
    this.pullWaitStats = Statistics.newInstances(numThreads);
    this.startTimes = new long[numThreads];
    final long currentTime = ticker.read();
    for (int i = 0; i < numThreads; ++i) {
      this.startTimes[i] = currentTime;
    }
    this.metricsCollector = metricsCollector;
    this.insertableMetricTracker = insertableMetricTracker;
    this.serverMetricsMsgSender = serverMetricsMsgSender;

    // Execute a thread to send metrics.
    Executors.newSingleThreadExecutor().submit(this::sendMetrics);
  }

  /**
   * Call after initializing threadPool.
   * @param numThreads The number of threads to run operations.
   */
  private Map<Integer, ServerThread<K, V>> initThreads(final int numThreads) {
    final Map<Integer, ServerThread<K, V>> initialized = new HashMap<>();

    LOG.log(Level.INFO, "Initializing {0} threads", numThreads);
    for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
      final ServerThread<K, V> thread = new ServerThread<>(queueSize, memoryStore);
      initialized.put(threadIndex, thread);
      threadPool.submit(thread);
    }
    return initialized;
  }

  @Override
  public void push(final K key, final P preValue, final int keyHash) {
    final HashedKey<K> hashedKey = new HashedKey<>(key, keyHash);
    final int blockId = blockResolver.resolveBlock(hashedKey);
    final int threadId = threadResolver.resolveThread(blockId);
    LOG.log(Level.FINEST, "Enqueue push request. Key: {0} BlockId: {1}, ThreadId: {2}, Hash: {3}",
        new Object[] {key, blockId, threadId, keyHash});
    threads.get(threadId).enqueue(new PushOp(hashedKey, preValue, threadId));
  }

  @Override
  public void pull(final K key, final String srcId, final int keyHash) {
    final HashedKey<K> hashedKey = new HashedKey<>(key, keyHash);
    final int blockId = blockResolver.resolveBlock(hashedKey);
    final int threadId = threadResolver.resolveThread(blockId);
    LOG.log(Level.FINEST, "Enqueue pull request. Key: {0} BlockId: {1}, ThreadId: {2}, Hash: {3}",
        new Object[] {key, blockId, threadId, keyHash});
    threads.get(threadId).enqueue(new PullOp(hashedKey, srcId, threadId));
  }

  /**
   * @return number of operations pending, on all queues
   */
  @Override
  public int opsPending() {
    int sum = 0;
    for (final ServerThread<K, V> partition : threads.values()) {
      sum += partition.opsPending();
    }
    return sum;
  }

  private void printStats(final int threadId, final long elapsedTime) {
    final Statistics pullStat = pullStats[threadId];
    final Statistics pushStat = pushStats[threadId];
    final Statistics requestStat = requestStats[threadId];
    final Statistics pushWaitStat = pushWaitStats[threadId];
    final Statistics pullWaitStat = pullWaitStats[threadId];

    LOG.log(Level.INFO, "PS Elapsed Time: {0}, PS Sum Pull: {1}, PS Avg Pull: {2}, PS Pull Count: {3}, " +
            "PS Sum Push: {4}, PS Avg Push: {5}, PS Push Count: {6}, " +
            "PS Avg Request: {7}, PS Sum Request: {8}, PS Request Count: {9}, " +
            "PS Avg Push Wait: {10}, PS Sum Push Wait: {11}, " +
            "PS Avg Pull Wait: {12}, PS Sum Pull Wait: {13}",
        new Object[]{elapsedTime / 1e9D, Double.toString(pullStat.sum()), Double.toString(pullStat.avg()),
            pullStat.count(), Double.toString(pushStat.sum()), Double.toString(pushStat.avg()), pushStat.count(),
            Double.toString(requestStat.avg()), Double.toString(requestStat.sum()), requestStat.count(),
            Double.toString(pushWaitStat.avg()), Double.toString(pushWaitStat.sum()),
            Double.toString(pullWaitStat.avg()), Double.toString(pullWaitStat.sum())});
    pushStat.reset();
    pullStat.reset();
    requestStat.reset();
    pushWaitStat.reset();
    pullWaitStat.reset();
    startTimes[threadId] = ticker.read();
  }

  /**
   * Sends metrics that have been collected within the current window.
   */
  private void sendMetrics() {
    try {
      Thread.sleep(METRIC_INIT_DELAY_MS);
      while (true) {
        insertableMetricTracker.put(ConstantsForServer.SERVER_PROCESSING_UNIT, getProcessingUnit());
        metricsCollector.stop();

        final int numPartitionBlocks = memoryStore.getNumBlocks();
        serverMetricsMsgSender.send(window, numPartitionBlocks);
        window++;
        Thread.sleep(logPeriod);
      }
    } catch (final MetricException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Computes processing unit (C_s_proc) across all threads in this Server.
   * {@code Double.POSITIVE_INFINITY} is returned when all threads
   * have not processed any pull requests so far.
   */
  private double getProcessingUnit() {
    double count = 0D;
    double sum = 0D;

    synchronized (pullStats) {
      for (final Statistics stat : pullStats) {
        count += stat.count();
        sum += stat.sum();
      }
    }

    if (count == 0D) {
      return Double.POSITIVE_INFINITY;
    } else {
      return sum / count;
    }
  }

  /**
   * A generic operation; operations are queued at each Partition.
   */
  private interface Op<K, V> {
    /**
     * Method to apply when dequeued by the Partition.
     */
    void apply();
  }

  /**
   * A push operation.
   */
  private class PushOp implements Op<K, V> {
    private final HashedKey<K> hashedKey;
    private final P preValue;
    private final long timestamp;
    private final int threadId;

    PushOp(final HashedKey<K> hashedKey, final P preValue, final int threadId) {
      this.hashedKey = hashedKey;
      this.preValue = preValue;
      this.timestamp = ticker.read();
      this.threadId = threadId;
    }

    /**
     * Read from MemoryStore, modify (update), and write to kvStore.
     */
    @Override
    public void apply() {
      try {
        final long waitEndTime = ticker.read();
        pushWaitStats[threadId].put(waitEndTime - timestamp);

        final Pair<HashedKey<K>, V> oldKVPair = memoryStore.get(hashedKey);

        final V oldValue;
        if (null == oldKVPair) {
          LOG.log(Level.FINE, "The value did not exist. Will use the initial value specified in ParameterUpdater.");
          oldValue = parameterUpdater.initValue(hashedKey.getKey());
        } else {
          oldValue = oldKVPair.getSecond();
        }

        final V deltaValue = parameterUpdater.process(hashedKey.getKey(), preValue);
        if (deltaValue == null) {
          return;
        }

        final V updatedValue = parameterUpdater.update(oldValue, deltaValue);
        memoryStore.put(hashedKey, updatedValue);

        final long processEndTime = ticker.read();
        final long processingTime = processEndTime - waitEndTime;
        pushStats[threadId].put(processingTime);
        requestStats[threadId].put(processingTime);

        final long elapsedTime = processEndTime - startTimes[threadId];
        if (logPeriod > 0 && elapsedTime > logPeriod) {
          printStats(threadId, elapsedTime);
        }

      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Exception occurred", e);
      }
    }
  }

  /**
   * A pull operation.
   */
  private class PullOp implements Op<K, V> {
    private final HashedKey<K> hashedKey;
    private final long timestamp;
    private final String srcId;
    private final int threadId;

    PullOp(final HashedKey<K> hashedKey, final String srcId, final int threadId) {
      this.hashedKey = hashedKey;
      this.srcId = srcId;
      this.timestamp = ticker.read();
      this.threadId = threadId;
    }

    /**
     * Read from MemoryStore and send the hashedKey-value pair to srcId.
     * To ensure atomicity, the hashedKey-value pair should be serialized immediately in sender.
     */
    @Override
    public void apply() {
      try {
        final long waitEndTime = ticker.read();
        pullWaitStats[threadId].put(waitEndTime - timestamp);

        final Pair<HashedKey<K>, V> kvPair = memoryStore.get(hashedKey);
        final V value;
        if (kvPair == null) {
          final V initValue = parameterUpdater.initValue(hashedKey.getKey());
          final Pair<HashedKey<K>, Boolean> result =
              memoryStore.put(hashedKey, initValue);
          final boolean isSuccess = result.getSecond();
          if (!isSuccess) {
            throw new RuntimeException("The data does not exist. Tried to put the initial value, but has failed");
          }
          value = initValue;
        } else {
          value = kvPair.getSecond();
        }
        sender.sendReplyMsg(srcId, hashedKey.getKey(), value);
        final long processEndTime = ticker.read();
        final long processingTime = processEndTime - waitEndTime;
        pullStats[threadId].put(processingTime);
        requestStats[threadId].put(processingTime);

        final long elapsedTime = processEndTime - startTimes[threadId];
        if (logPeriod > 0 && elapsedTime > logPeriod) {
          printStats(threadId, elapsedTime);
        }
      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Exception occurred", e);
      }
    }
  }

  /**
   * All push and pull operations should be sent to the appropriate partition.
   * Each partition is assigned to a thread, whose processing loop dequeues and applies operations to its local kvStore.
   * A partition is queued and handled by single thread, which ensures that all operations on a hashedKey
   * are performed atomically, in order (no updates can be lost).
   *
   * The single queue-and-thread design provides a simple guarantee of atomicity for applying operations.
   * It also means pull operations are queued behind push operations.
   * This ensures that pull operations return with up-to-date information (for a single client, this
   * is basically read-your-writes).
   * However, it also means that pull operations may take awhile to process.
   * Workers block for pulls, while sending pushes asynchronously.
   * We should further explore this trade-off with real ML workloads.
   */
  private static class ServerThread<K, V> implements Runnable {
    private static final long QUEUE_TIMEOUT_MS = 3000;
    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.
    private final MemoryStore<HashedKey<K>> memoryStore;

    private volatile boolean shutdown = false;

    ServerThread(final int queueSize, final MemoryStore<HashedKey<K>> memoryStore) {
      this.queue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize / 10;
      this.localOps = new ArrayList<>(drainSize);
      this.memoryStore = memoryStore;
    }

    /**
     * Enqueue an operation onto the queue, blocking if the queue is full.
     * When the queue is full, this method will block; thus, a full queue will block the thread calling
     * enqueue, e.g., from the NCS message thread pool, until the queue is drained. This seems reasonable,
     * as it will block client messages from being processed and overloading the system.
     *
     * An alternative would be to send a "busy" message in response, and have
     * the client resend the operation. This will require changes in the client as well.
     *
     * @param op the operation to enqueue
     */
    void enqueue(final Op<K, V> op) {
      try {
        queue.put(op);
      } catch (final InterruptedException e) {
        LOG.log(Level.FINEST, "Enqueue failed with InterruptedException", e);
      }
    }

    /**
     * @return number of pending operations in the queue.
     */
    int opsPending() {
      return queue.size();
    }

    @Override
    public void run() {
      while (!shutdown) {
        // First, poll and apply. The timeout allows the run thread to shutdown cleanly within timeout ms.
        try {
          final Op<K, V> op = queue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (op == null) {
            continue;
          }
          op.apply();
        } catch (final InterruptedException e) {
          LOG.log(Level.FINEST, "Poll failed with InterruptedException", e);
          continue;
        }

        // Then, drain up to LOCAL_OPS_SIZE of the remaining queue and apply.
        // Calling drainTo does not block if queue is empty, which is why we poll first.
        // This should be faster than polling each op, because the blocking queue's lock is only acquired once.
        queue.drainTo(localOps, drainSize);
        for (final Op<K, V> op : localOps) {
          op.apply();
        }
        localOps.clear();
      }
    }

    /**
     * Cleanly shutdown the run thread.
     */
    public void shutdown() {
      shutdown = true;
    }
  }

  /**
   * Associates blocks with threads, to distribute load across threads while guaranteeing operations on one block
   * to be processed by only one thread.
   * The current implementation allocates unseen block to threads in a round-robin fashion.
   * Note that the load is not perfectly distributed evenly, because the blocks that have moved out are not considered.
   */
  private static class ThreadResolver {
    private final int numThreads;
    private AtomicInteger nextIndex = new AtomicInteger(0);
    private Map<Integer, Integer> blockToThread = new HashMap<>();

    ThreadResolver(final int numThreads) {
      this.numThreads = numThreads;
    }

    int resolveThread(final int blockId) {
      final Integer threadId = blockToThread.get(blockId);
      if (null == threadId) {
        return assignThread(blockId);
      } else {
        return threadId;
      }
    }

    private synchronized int assignThread(final int blockId) {
      final Integer threadId = blockToThread.get(blockId);
      if (threadId != null) {
        return threadId;
      }
      final int newThreadId = nextIndex.getAndIncrement() % numThreads;
      blockToThread.put(blockId, newThreadId);
      LOG.log(Level.FINEST, "BlockId {0} / ThreadId {1}", new Object[] {blockId, newThreadId});
      return newThreadId;
    }
  }
}
