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
package edu.snu.cay.services.ps.server.impl.fixed;

import com.google.common.base.Ticker;
import edu.snu.cay.services.ps.common.Statistics;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.services.ps.server.api.ServerSideMsgSender;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.parameters.ServerMetricsWindowMs;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.htrace.TraceInfo;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of Parameter Server, whose partitions are fixed at their initial servers.
 * Receives push and pull operations from (e.g., from the network) and immediately queues them.
 * The processing loop in each thread applies these operations in order; for pull operations
 * this results in a send call via {@link ServerSideMsgSender}.
 * For more information about the implementation, see {@link ServerThread}.
 *
 * Supports a static number of partitions (the number of partitions is fixed at construction time).
 */
@EvaluatorSide
public final class StaticParameterServer<K, P, V> implements ParameterServer<K, P, V> {
  private static final Logger LOG = Logger.getLogger(StaticParameterServer.class.getName());

  /**
   * Initial delay before sending the first metric.
   */
  private static final long METRIC_INIT_DELAY_MS = 3000;

  /**
   * ServerResolver that maps hashed keys to partitions.
   */
  private final ServerResolver serverResolver;

  /**
   * The number of threads to run operations.
   */
  private final int numThreads;

  /**
   * Max size of each thread's queue.
   */
  private final int queueSize;

  /**
   * The list of local partitions.
   */
  private final List<Integer> localPartitions;

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
  private final ServerSideMsgSender<K, P, V> sender;

  /**
   * Statistics of the processing time of push operation.
   */
  private final Statistics[] pushStats;

  /**
   * Statistics of the processing time of pull operation.
   */
  private final Statistics[] pullStats;

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
   * Length of window, which is discrete time period to send metrics (in ms).
   */
  private final long metricsWindowMs;

  /**
   * The current index of window.
   */
  private int windowIndex = 0;

  @Inject
  private StaticParameterServer(@Parameter(EndpointId.class) final String endpointId,
                                @Parameter(ServerNumThreads.class) final int numThreads,
                                @Parameter(ServerQueueSize.class) final int queueSize,
                                @Parameter(ServerMetricsWindowMs.class) final long metricsWindowMs,
                                final ServerResolver serverResolver,
                                final ParameterUpdater<K, P, V> parameterUpdater,
                                final ServerSideMsgSender<K, P, V> sender) {
    this.numThreads = numThreads;
    this.localPartitions = serverResolver.getPartitions(endpointId);
    this.serverResolver = serverResolver;
    this.queueSize = queueSize;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.threads = initThreads();
    this.parameterUpdater = parameterUpdater;
    this.sender = sender;
    this.pushStats = Statistics.newInstances(numThreads);
    this.pullStats = Statistics.newInstances(numThreads);
    this.pushWaitStats = Statistics.newInstances(numThreads);
    this.pullWaitStats = Statistics.newInstances(numThreads);
    this.startTimes = new long[numThreads];
    final long currentTime = ticker.read();
    for (int i = 0; i < numThreads; ++i) {
      this.startTimes[i] = currentTime;
    }
    this.metricsWindowMs = metricsWindowMs;

    // Execute a thread to send metrics.
    Executors.newSingleThreadExecutor().submit(this::logMetrics);
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
  public void push(final K key, final P preValue, final int keyHash) {
    final int partitionId = serverResolver.resolvePartition(keyHash);
    final int threadId = localPartitions.indexOf(partitionId) % numThreads;
    threads.get(threadId).enqueue(new PushOp(key, preValue, threadId));
  }

  @Override
  public void pull(final K key, final String requesterId, final int keyHash, final int requestId,
                   @Nullable final TraceInfo traceInfo) {
    final int partitionId = serverResolver.resolvePartition(keyHash);
    final int threadId = localPartitions.indexOf(partitionId) % numThreads;
    threads.get(threadId).enqueue(new PullOp(key, requesterId, threadId, requestId));
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
   * Close the server after processing all the queued operations.
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
   * Logs metrics that have been collected within the current window.
   */
  private void logMetrics() {
    try {
      // Sleep to skip the initial metrics that have been collected while the server being set up.
      Thread.sleep(METRIC_INIT_DELAY_MS);

      while (true) {
        Thread.sleep(metricsWindowMs);

        // After time has elapsed as long as a windowIndex, get the collected metrics and build a MetricsMessage.
        final Pair<Integer, Double> totalPullStat = summarizeAndResetStats(pullStats);
        final Pair<Integer, Double> totalPushStat = summarizeAndResetStats(pushStats);
        final Pair<Integer, Double> totalPullWaitStats = summarizeAndResetStats(pullWaitStats);
        final Pair<Integer, Double> totalPushWaitStats = summarizeAndResetStats(pushWaitStats);

        final ServerMetrics metricsMessage = ServerMetrics.newBuilder()
            .setWindowIndex(windowIndex)
            .setNumModelBlocks(0) // EM is not used here.
            .setMetricWindowMs(metricsWindowMs)
            .setTotalPullProcessed(totalPullStat.getFirst())
            .setTotalPushProcessed(totalPushStat.getFirst())
            .setTotalPullProcessingTimeSec(totalPullStat.getSecond() / 1e9D)
            .setTotalPushProcessingTimeSec(totalPushStat.getSecond() / 1e9D)
            .setTotalPullWaitingTimeSec(totalPullWaitStats.getSecond() / 1e9D)
            .setTotalPushWaitingTimeSec(totalPushWaitStats.getSecond() / 1e9D)
            .build();

        LOG.log(Level.FINE, "ServerMetrics {0}", metricsMessage);

        windowIndex++;
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Exception Occurred", e); // Log for the case when the thread swallows the exception
      throw new RuntimeException(e);
    }
  }

  /**
   * Computes the total number and time spent on processing requests with the {@link ServerThread}s in this server.
   * Summarizes the statistics (count, time) across all {@link ServerThread}s in this server,
   * and resets the stat to collect metrics for the next window.
   */
  private Pair<Integer, Double> summarizeAndResetStats(final Statistics[] stats) {
    int processedCount = 0;
    double procTimeSum = 0D;

    for (final Statistics stat : stats) {
      processedCount += stat.count();
      procTimeSum += stat.sum();
      stat.reset();
    }

    return new Pair<>(processedCount, procTimeSum);
  }

  /**
   * A generic operation; operations are queued at each Partition.
   */
  private interface Op<K, V> {
    /**
     * Method to apply when dequeued by the Partition.
     * @param kvStore the raw kvStore map, provided by the Partition.
     */
    void apply(Map<K, V> kvStore);
  }

  /**
   * A push operation.
   */
  private class PushOp implements Op<K, V> {
    private final K key;
    private final P preValue;
    private final long timestamp;
    private final int threadId;

    PushOp(final K key, final P preValue, final int threadId) {
      this.key = key;
      this.preValue = preValue;
      this.timestamp = ticker.read();
      this.threadId = threadId;
    }

    /**
     * Read from kvStore, modify (update), and write to kvStore.
     */
    @Override
    public void apply(final Map<K, V> kvStore) {
      final long waitEndTime = ticker.read();
      final long waitTime = waitEndTime - timestamp;
      pushWaitStats[threadId].put(waitTime);

      if (!kvStore.containsKey(key)) {
        kvStore.put(key, parameterUpdater.initValue(key));
      }

      final V deltaValue = parameterUpdater.process(key, preValue);
      if (deltaValue == null) {
        return;
      }

      final V updatedValue = parameterUpdater.update(kvStore.get(key), deltaValue);
      kvStore.put(key, updatedValue);

      final long processEndTime = ticker.read();
      final long processingTime = processEndTime - waitEndTime;
      pushStats[threadId].put(processingTime);
    }
  }

  /**
   * A pull operation.
   */
  private class PullOp implements Op<K, V> {
    private final K key;
    private final String srcId;
    private final long timestamp;
    private final int threadId;
    private final int requestId;

    PullOp(final K key, final String srcId, final int threadId, final int requestId) {
      this.key = key;
      this.srcId = srcId;
      this.timestamp = ticker.read();
      this.threadId = threadId;
      this.requestId = requestId;
    }

    /**
     * Read from kvStore and send the key-value pair to srcId.
     * To ensure atomicity, the key-value pair should be serialized immediately in sender.
     */
    @Override
    public void apply(final Map<K, V> kvStore) {
      final long waitEndTime = ticker.read();
      final long waitTime = waitEndTime - timestamp;
      pullWaitStats[threadId].put(waitTime);

      if (!kvStore.containsKey(key)) {
        kvStore.put(key, parameterUpdater.initValue(key));
      }

      // The request's time spent in queue + processing time before sending a reply.
      final long elapsedTimeInServer = ticker.read() - timestamp;
      sender.sendPullReplyMsg(srcId, key, kvStore.get(key), requestId, elapsedTimeInServer, null);

      final long processEndTime = ticker.read();

      // Elapsed time since the request has been dequeued.
      final long actualProcessingTime = processEndTime - waitEndTime;
      pullStats[threadId].put(actualProcessingTime);
    }
  }

  /**
   * All push and pull operations should be sent to the appropriate partition.
   * Each partition is assigned to a thread, whose processing loop dequeues and applies operations to its local kvStore.
   * A partition is queued and handled by single thread, which ensures that all operations on a key
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

    private final Map<K, V> kvStore;
    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.

    private final StateMachine stateMachine;

    ServerThread(final int queueSize) {
      this.kvStore = new HashMap<>();
      this.queue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize / 10;
      this.localOps = new ArrayList<>(drainSize);
      this.stateMachine = initStateMachine();
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
            op.apply(kvStore);
          } catch (final InterruptedException e) {
            LOG.log(Level.WARNING, "Poll failed with InterruptedException", e);
            continue;
          }

          // Then, drain up to LOCAL_OPS_SIZE of the remaining queue and apply.
          // Calling drainTo does not block if queue is empty, which is why we poll first.
          // This should be faster than polling each op, because the blocking queue's lock is only acquired once.
          queue.drainTo(localOps, drainSize);
          for (final Op<K, V> op : localOps) {
            op.apply(kvStore);
          }
          localOps.clear();
        }
      } catch (final RuntimeException e) {
        LOG.log(Level.SEVERE, "PS server thread has been down due to RuntimeException", e);
        throw e;
      }

      finishClose();
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
