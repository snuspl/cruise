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
import edu.snu.cay.common.metric.*;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.common.Statistics;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import edu.snu.cay.services.ps.metric.avro.ServerThreadMetrics;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.services.ps.server.api.ServerSideReplySender;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.parameters.ServerMetricsWindowMs;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import edu.snu.cay.utils.StateMachine;
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
  private final ServerSideReplySender<K, P, V> sender;

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
   * Number of threads working in the server.
   */
  private int numThreads;

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
   * Statistics of the waiting time of push operation.
   */
  private final Statistics[] requestWaitStats;

  /**
   * Bookkeeping start time of the processing threads.
   */
  private long[] startTimes;

  /**
   * Ticker to track the time.
   */
  private final Ticker ticker = Ticker.systemTicker();

  /**
   * Sends MetricsMessage that consists of Metrics and additional information
   * such as windowIndex, and the number of partition blocks in the local MemoryStore.
   */
  private final MetricsMsgSender<ServerMetrics> metricsMsgSender;

  /**
   * Length of window, which is discrete time period to send metrics (in ms).
   */
  private final long metricsWindowMs;

  /**
   * The current index of window.
   */
  private int windowIndex = 0;


  @Inject
  private DynamicParameterServer(final MemoryStore<HashedKey<K>> memoryStore,
                                 final BlockResolver<HashedKey<K>> blockResolver,
                                 @Parameter(ServerNumThreads.class) final int numThreads,
                                 @Parameter(ServerQueueSize.class) final int queueSize,
                                 @Parameter(ServerMetricsWindowMs.class) final long metricsWindowMs,
                                 final MetricsMsgSender<ServerMetrics> metricsMsgSender,
                                 final ParameterUpdater<K, P, V> parameterUpdater,
                                 final ServerSideReplySender<K, P, V> sender) {
    this.memoryStore = memoryStore;
    this.blockResolver = blockResolver;
    this.queueSize = queueSize;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.threads = initThreads();
    this.parameterUpdater = parameterUpdater;
    this.sender = sender;
    this.numThreads = numThreads;
    this.threadResolver = new ThreadResolver(numThreads);
    this.pushStats = Statistics.newInstances(numThreads);
    this.pullStats = Statistics.newInstances(numThreads);
    this.requestStats = Statistics.newInstances(numThreads);
    this.pushWaitStats = Statistics.newInstances(numThreads);
    this.pullWaitStats = Statistics.newInstances(numThreads);
    this.requestWaitStats = Statistics.newInstances(numThreads);
    this.startTimes = new long[numThreads];
    final long currentTime = ticker.read();
    for (int i = 0; i < numThreads; ++i) {
      this.startTimes[i] = currentTime;
    }
    this.metricsMsgSender = metricsMsgSender;
    this.metricsWindowMs = metricsWindowMs;

    // Execute a thread to send metrics.
    Executors.newSingleThreadExecutor().submit(this::sendMetrics);
  }

  /**
   * Call after initializing threadPool.
   */
  private Map<Integer, ServerThread<K, V>> initThreads() {
    final Map<Integer, ServerThread<K, V>> initialized = new HashMap<>();

    LOG.log(Level.INFO, "Initializing {0} threads", numThreads);
    for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
      final ServerThread<K, V> thread = new ServerThread<>(queueSize);
      initialized.put(threadIndex, thread);
      threadPool.submit(thread);
    }
    return initialized;
  }

  @Override
  public void push(final K key, final P preValue, final String srcId, final int keyHash) {
    final HashedKey<K> hashedKey = new HashedKey<>(key, keyHash);
    final int blockId = blockResolver.resolveBlock(hashedKey);
    final int threadId = threadResolver.resolveThread(blockId);
    LOG.log(Level.FINEST, "Enqueue push request. Key: {0} BlockId: {1}, ThreadId: {2}, Hash: {3}",
        new Object[] {key, blockId, threadId, keyHash});
    threads.get(threadId).enqueue(new PushOp(hashedKey, preValue, srcId, threadId));
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

  /**
   * logs stats (pull, push, request times and wait times) of a working thread.
   * @param threadId of the thread
   */
  private void logThreadStats(final int threadId) {
    final long timeSinceLastPrintStat = ticker.read() - startTimes[threadId];
    final Statistics pullStat = pullStats[threadId];
    final Statistics pushStat = pushStats[threadId];
    final Statistics requestStat = requestStats[threadId];
    final Statistics pushWaitStat = pushWaitStats[threadId];
    final Statistics pullWaitStat = pullWaitStats[threadId];
    final Statistics requestWaitStat = requestWaitStats[threadId];

    final ServerThreadMetrics threadMetrics = ServerThreadMetrics.newBuilder()
        .setThreadId(threadId)
        .setNumPendingOps(threads.get(threadId).opsPending())
        .setTotalTime(timeSinceLastPrintStat / 1e9D)
        .setPullCount((int)pullStat.count())
        .setTotalPullTime(pullStat.sum())
        .setAvgPullTime(pullStat.avg())
        .setTotalPullWaitTime(pullWaitStat.sum())
        .setAvgPullWaitTime(pullWaitStat.avg())
        .setPushCount((int)pushStat.count())
        .setTotalPushTime(pushStat.sum())
        .setAvgPushTime(pushStat.avg())
        .setTotalPushWaitTime(pushWaitStat.sum())
        .setAvgPushWaitTime(pushWaitStat.avg())
        .setReqCount((int)requestStat.count())
        .setTotalReqTime(requestStat.sum())
        .setAvgReqTime(requestStat.avg())
        .setTotalReqWaitTime(requestWaitStat.sum())
        .setAvgReqWaitTime(requestWaitStat.avg())
        .build();

    LOG.log(Level.FINE, "ServerThreadMetrics {0}", threadMetrics);

    startTimes[threadId] = ticker.read();
  }

  /**
   * Sends metrics that have been collected within the current windowIndex.
   */
  private void sendMetrics() {
    try {
      // Sleep to skip the initial metrics that have been collected while the server being set up.
      Thread.sleep(METRIC_INIT_DELAY_MS);

      while (true) {
        Thread.sleep(metricsWindowMs);

        // After time has elapsed as long as a windowIndex, get the collected metrics and build a MetricsMessage.
        final double avgPullTime = getAvgProcTimePerReq(pullStats);
        final double avgPushTime = getAvgProcTimePerReq(pushStats);
        final double avgReqProcTime = getAvgProcTimePerReq(requestStats);
        resetStats();

        // Send meaningful metrics only (i.e., infinity processing time implies that no data has been processed yet).
        if (avgPullTime != Double.POSITIVE_INFINITY && avgPushTime != Double.POSITIVE_INFINITY) {
          final int numPartitionBlocks = memoryStore.getNumBlocks();
          final ServerMetrics metricsMessage = ServerMetrics.newBuilder()
              .setWindowIndex(windowIndex)
              .setNumPartitionBlocks(numPartitionBlocks)
              .setMetricWindowMs(metricsWindowMs)
              .setAvgPullProcessingTime(avgPullTime)
              .setAvgPushProcessingTime(avgPushTime)
              .setAvgReqProcessingTime(avgReqProcTime)
              .build();

          LOG.log(Level.FINE, "Sending ServerMetrics {0}", metricsMessage);
          metricsMsgSender.send(metricsMessage);
        }
        windowIndex++;
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Exception Occurred", e); // Log for the case when the thread swallows the exception
      throw new RuntimeException(e);
    }
  }

  /**
   * Computes processing unit (C_s_proc) across all threads in this Server.
   * It is computed by first calculating the total throughput of this server by adding each thread's throughput
   * and getting the inverse of the throughput to finally get the time required to process a unit request.
   *
   * {@code Double.POSITIVE_INFINITY} is returned when all threads
   * have not processed any requests so far.
   */
  private double getAvgProcTimePerReq(final Statistics[] procTimeStats) {
    double throughputSum = 0D;

    synchronized (procTimeStats) {
      for (final Statistics stat : procTimeStats) {
        throughputSum += stat.count() / stat.sum();
      }
    }

    if (throughputSum == 0D) {
      return Double.POSITIVE_INFINITY;
    } else {
      return 1.0 / throughputSum;
    }
  }

  /**
   * Resets all {@link Statistics} for the next round of metrics.
   */
  private void resetStats() {
    for (int threadIdx = 0; threadIdx < numThreads; threadIdx++) {
      logThreadStats(threadIdx);
      pullStats[threadIdx].reset();
      pullWaitStats[threadIdx].reset();
      pushStats[threadIdx].reset();
      pushWaitStats[threadIdx].reset();
      requestStats[threadIdx].reset();
      requestWaitStats[threadIdx].reset();
    }
  }

  /**
   * Close the server. All the queued operations are rejected, and sent back to the worker who send the requests.
   */
  @Override
  public void close(final long timeoutMs) throws InterruptedException, TimeoutException, ExecutionException {

    final Future result = Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        // Close all threads
        for (final ServerThread thread : threads.values()) {
          thread.startClose();
        }

        // Wait for close to complete on all threads
        for (final ServerThread thread : threads.values()) {
          thread.waitForClose();
        }
      }
    });

    result.get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * A generic operation; operations are queued at each ServerThread.
   */
  private interface Op<K, V> {

    /**
     * Method to apply when dequeued by the ServerThread.
     */
    void apply();

    /**
     * Method to reject the operation when closing the ServerThread.
     */
    void reject();
  }

  /**
   * A push operation.
   */
  private class PushOp implements Op<K, V> {
    private final HashedKey<K> hashedKey;
    private final P preValue;
    private final String srcId;
    private final long timestamp;
    private final int threadId;

    PushOp(final HashedKey<K> hashedKey, final P preValue, final String srcId, final int threadId) {
      this.hashedKey = hashedKey;
      this.preValue = preValue;
      this.srcId = srcId;
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
        final long waitTime = waitEndTime - timestamp;
        pushWaitStats[threadId].put(waitTime);
        requestWaitStats[threadId].put(waitTime);

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
      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Exception occurred", e);
      }
    }

    @Override
    public void reject() {
      sender.sendPushRejectMsg(srcId, hashedKey.getKey(), preValue);
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
        final long waitTime = waitEndTime - timestamp;
        pullWaitStats[threadId].put(waitTime);
        requestWaitStats[threadId].put(waitTime);

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
        sender.sendPullReplyMsg(srcId, hashedKey.getKey(), value);
        final long processEndTime = ticker.read();
        final long processingTime = processEndTime - waitEndTime;
        pullStats[threadId].put(processingTime);
        requestStats[threadId].put(processingTime);
      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Exception occurred", e);
      }
    }

    @Override
    public void reject() {
      sender.sendPullRejectMsg(srcId, hashedKey.getKey());
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
    private static final String STATE_RUNNING = "RUNNING";
    private static final String STATE_CLOSING = "CLOSING";
    private static final String STATE_CLOSED = "CLOSED";

    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.

    private final StateMachine stateMachine;

    ServerThread(final int queueSize) {
      this.queue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize / 10;
      this.localOps = new ArrayList<>(drainSize);
      this.stateMachine = initStateMachine();
    }

    private StateMachine initStateMachine() {
      return StateMachine.newBuilder()
          .addState(STATE_RUNNING, "Server thread is running. It executes operations in the queue.")
          .addState(STATE_CLOSING, "Server thread is closing. It will be closed after rejecting whole remaining ops.")
          .addState(STATE_CLOSED, "Server thread is closed. It finished rejecting whole remaining operations.")
          .addTransition(STATE_RUNNING, STATE_CLOSING, "Time to close the thread.")
          .addTransition(STATE_CLOSING, STATE_CLOSED, "Closing the thread is done.")
          .setInitialState(STATE_RUNNING)
          .build();
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
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
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
      while (stateMachine.getCurrentState().equals(STATE_RUNNING)) {
        // First, poll and apply. The timeout allows the run thread to close cleanly within timeout ms.
        try {
          final Op<K, V> op = queue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (op == null) {
            continue;
          }
          op.apply();
        } catch (final InterruptedException e) {
          LOG.log(Level.WARNING, "Poll failed with InterruptedException", e);
          continue;
        }

        // Then, drain up to LOCAL_OPS_SIZE of the remaining queue and apply.
        // Calling drainTo does not block if queue is empty, which is why we poll first.
        // This should be faster than polling each op, because the blocking queue's lock is only acquired once.
        queue.drainTo(localOps, drainSize);
        localOps.forEach(Op::apply);
        localOps.clear();
      }

      // reject all operations in the queue before exit
      while (!queue.isEmpty()) {
        queue.drainTo(localOps, drainSize);
        localOps.forEach(Op::reject);
        localOps.clear();
      }

      finishClose();
    }

    /**
     * Start closing the thread.
     * The thread will be closed after sending reject messages for all pending operations.
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
