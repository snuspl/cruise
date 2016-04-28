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

import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.services.ps.server.api.ServerSideReplySender;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.server.parameters.ServerLogPeriod;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * this results in a send call via {@link ServerSideReplySender}.
 * For more information about the implementation, see {@link ServerThread}.
 *
 * Supports a static number of partitions (the number of partitions is fixed at construction time).
 */
@EvaluatorSide
public final class StaticParameterServer<K, P, V> implements ParameterServer<K, P, V> {
  private static final Logger LOG = Logger.getLogger(StaticParameterServer.class.getName());

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
  private final ServerSideReplySender<K, V> sender;

  private final int logPeriod;
  private final Statistics pushStat = new Statistics();
  private final Statistics pullStat = new Statistics();
  private final Statistics requestStat = new Statistics();
  private final Statistics waitStat = new Statistics();
  private long startTime = System.currentTimeMillis();

  private static class Statistics {
    private long sum = 0;
    private int count = 0;

    void put(final long value) {
      sum += value;
      ++count;
    }

    void reset() {
      sum = 0;
      count = 0;
    }

    double avg() {
      return sum() / count;
    }

    double sum() {
      return sum / 1000.0D;
    }

    double count() {
      return count;
    }
  }

  private void printStats(final long elapsedTime) {
    LOG.log(Level.INFO, "PS Elapsed Time: {0}, PS Sum Pull: {1}, PS Avg Pull: {2}, PS Pull Count: {3}, " +
            "PS Sum Push: {4}, PS Avg Push: {5}, PS Push Count: {6}, " +
            "PS Avg Request: {7}, PS Sum Request: {8}, PS Request Count: {9}, " +
            "PS Avg Wait: {10}, PS Sum Wait: {11}",
        new Object[]{elapsedTime / 1000D, Double.toString(pullStat.sum()), Double.toString(pullStat.avg()),
            pullStat.count(), Double.toString(pushStat.sum()), Double.toString(pushStat.avg()), pushStat.count(),
            Double.toString(requestStat.avg()), Double.toString(requestStat.sum()), requestStat.count(),
            Double.toString(waitStat.avg()), Double.toString(waitStat.sum())});
    pushStat.reset();
    pullStat.reset();
    requestStat.reset();
    waitStat.reset();
    startTime = System.currentTimeMillis();
  }

  @Inject
  private StaticParameterServer(@Parameter(EndpointId.class) final String endpointId,
                                @Parameter(ServerNumThreads.class) final int numThreads,
                                @Parameter(ServerQueueSize.class) final int queueSize,
                                @Parameter(ServerLogPeriod.class) final int logPeriod,
                                final ServerResolver serverResolver,
                                final ParameterUpdater<K, P, V> parameterUpdater,
                                final ServerSideReplySender<K, V> sender) {
    this.numThreads = numThreads;
    this.localPartitions = serverResolver.getPartitions(endpointId);
    this.serverResolver = serverResolver;
    this.queueSize = queueSize;
    this.logPeriod = logPeriod;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.threads = initThreads();
    this.parameterUpdater = parameterUpdater;
    this.sender = sender;
    LOG.log(Level.INFO, "Log Period = {0} millisecond", logPeriod);
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
    threads.get(threadId).enqueue(new PushOp(key, preValue));
  }

  @Override
  public void pull(final K key, final String srcId, final int keyHash) {
    final int partitionId = serverResolver.resolvePartition(keyHash);
    final int threadId = localPartitions.indexOf(partitionId) % numThreads;
    threads.get(threadId).enqueue(new PullOp(key, srcId));
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

    public PushOp(final K key, final P preValue) {
      this.key = key;
      this.preValue = preValue;
      this.timestamp = System.currentTimeMillis();
    }

    /**
     * Read from kvStore, modify (update), and write to kvStore.
     */
    @Override
    public void apply(final Map<K, V> kvStore) {
      final long waitEndTime = System.currentTimeMillis();
      waitStat.put(waitEndTime - timestamp);
      if (!kvStore.containsKey(key)) {
        kvStore.put(key, parameterUpdater.initValue(key));
      }

      final V deltaValue = parameterUpdater.process(key, preValue);
      if (deltaValue == null) {
        return;
      }

      final V updatedValue = parameterUpdater.update(kvStore.get(key), deltaValue);
      kvStore.put(key, updatedValue);

      final long processEndTime = System.currentTimeMillis();
      final long processingTime = processEndTime - waitEndTime;
      pushStat.put(processingTime);
      requestStat.put(processingTime);

      final long elapsedTime = processEndTime - startTime;
      if (logPeriod > 0 && elapsedTime > logPeriod) {
        printStats(elapsedTime);
      }
    }
  }

  /**
   * A pull operation.
   */
  private class PullOp implements Op<K, V> {
    private final K key;
    private final String srcId;
    private final long timestamp;

    public PullOp(final K key, final String srcId) {
      this.key = key;
      this.srcId = srcId;
      this.timestamp = System.currentTimeMillis();
    }

    /**
     * Read from kvStore and send the key-value pair to srcId.
     * To ensure atomicity, the key-value pair should be serialized immediately in sender.
     */
    @Override
    public void apply(final Map<K, V> kvStore) {
      final long waitEndTime = System.currentTimeMillis();
      waitStat.put(waitEndTime - timestamp);

      if (!kvStore.containsKey(key)) {
        kvStore.put(key, parameterUpdater.initValue(key));
      }

      sender.sendReplyMsg(srcId, key, kvStore.get(key));

      final long processEndTime = System.currentTimeMillis();
      final long processingTime = processEndTime - waitEndTime;
      pullStat.put(processingTime);
      requestStat.put(processingTime);

      final long elapsedTime = processEndTime - startTime;
      if (logPeriod > 0 && elapsedTime > logPeriod) {
        printStats(elapsedTime);
      }
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

    private final Map<K, V> kvStore;
    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.

    private volatile boolean shutdown = false;

    public ServerThread(final int queueSize) {
      this.kvStore = new HashMap<>();
      this.queue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize / 10;
      this.localOps = new ArrayList<>(drainSize);
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
    public void enqueue(final Op<K, V> op) {
      try {
        queue.put(op);
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
      }
    }

    /**
     * @return number of pending operations in the queue.
     */
    public int opsPending() {
      return queue.size();
    }

    /**
     * Loop that dequeues operations and applies them.
     * Dequeues are only performed through this thread.
     */
    @Override
    public void run() {
      while (!shutdown) {
        // First, poll and apply. The timeout allows the run thread to shutdown cleanly within timeout ms.
        try {
          final Op<K, V> op = queue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (op == null) {
            continue;
          }
          op.apply(kvStore);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
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
    }

    /**
     * Cleanly shutdown the run thread.
     */
    public void shutdown() {
      shutdown = true;
    }
  }
}
