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
package edu.snu.cay.services.ps.server.partitioned;

import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerQueueSize;
import org.apache.commons.collections.functors.ExceptionPredicate;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Partitioned Parameter Server, where partitions are dynamically moving in and out.
 */
public final class DynamicPartitionedParameterServer<K, P, V> {
  private static final Logger LOG = Logger.getLogger(DynamicPartitionedParameterServer.class.getName());
  private static final String DATA_TYPE = "SERVER_DATA";

  /**
   * The number of threads to run operations.
   */
  private final int numThreads;

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
  private final PartitionedServerSideReplySender<K, V> sender;

  /**
   * MemoryStore instance to access the data.
   */
  private final MemoryStore<K> memoryStore;

  /**
   * Object for resolving the block id assigned to given data.
   */
  private final BlockResolver<K> blockResolver;

  /**
   * Object for assigning a thread per block.
   */
  private final ThreadResolver threadResolver;

  @Inject
  private DynamicPartitionedParameterServer(final MemoryStore<K> memoryStore,
                                            final BlockResolver<K> blockResolver,
                                            @Parameter(ServerNumThreads.class)final int numThreads,
                                            @Parameter(ServerQueueSize.class) final int queueSize,
                                            final ParameterUpdater<K, P, V> parameterUpdater,
                                            final PartitionedServerSideReplySender<K, V> sender) {
    this.memoryStore = memoryStore;
    this.blockResolver = blockResolver;
    this.numThreads = numThreads;
    this.queueSize = queueSize;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.threads = initThreads();
    this.parameterUpdater = parameterUpdater;
    this.sender = sender;
    this.threadResolver = new ThreadResolver(numThreads);
  }

  /**
   * Call after initializing threadPool.
   */
  private Map<Integer, ServerThread<K, V>> initThreads() {
    final Map<Integer, ServerThread<K, V>> initialized = new HashMap<>();

    LOG.log(Level.INFO, "Initializing {0} threads", numThreads);
    for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
      final ServerThread<K, V> thread = new ServerThread<>(queueSize, memoryStore);
      initialized.put(threadIndex, thread);
      threadPool.submit(thread);
    }
    return initialized;
  }

  /**
   * Process a {@code preValue} sent from a worker and store the resulting value.
   * Uses {@link ParameterUpdater} to generate a value from {@code preValue} and to apply the generated value to
   * the k-v store.
   *
   * The push operation is enqueued to the queue that is assigned to its partition and returned immediately.
   *
   * @param key key object that {@code preValue} is associated with
   * @param preValue preValue sent from the worker
   * @param keyHash hash of the key, a positive integer used to map to the correct partition
   */
  public void push(final K key, final P preValue, final int keyHash) {
    final int blockId = blockResolver.resolveBlock(key);
    final int threadId = threadResolver.resolveThread(blockId);
    LOG.log(Level.FINEST, "Enqueue push request. Key: {0} BlockId: {1}, ThreadId: {2}, Hash: {3}",
        new Object[] {key, blockId, threadId, keyHash});
    threads.get(threadId).enqueue(new PushOp(key, preValue));
  }

  /**
   * Reply to srcId via {@link PartitionedServerSideReplySender}
   * with the value corresponding to the key.
   *
   * The pull operation is enqueued to the queue that is assigned to its partition and returned immediately.
   *
   * @param key key object that the requested {@code value} is associated with
   * @param srcId network Id of the requester
   * @param keyHash hash of the key, a positive integer used to map to the correct partition
   */
  public void pull(final K key, final String srcId, final int keyHash) {
    final int blockId = blockResolver.resolveBlock(key);
    final int threadId = threadResolver.resolveThread(blockId);
    LOG.log(Level.FINEST, "Enqueue pull request. Key: {0} BlockId: {1}, ThreadId: {2}, Hash: {3}",
        new Object[] {key, blockId, threadId, keyHash});
    threads.get(threadId).enqueue(new PullOp(key, srcId));
  }

  /**
   * A generic operation; operations are queued at each Partition.
   */
  private interface Op<K, V> {
    /**
     * Method to apply when dequeued by the Partition.
     * @param memoryStore MemoryStore to store the data
     */
    void apply(MemoryStore<K> memoryStore);
  }

  /**
   * A push operation.
   */
  private class PushOp implements Op<K, V> {
    private final K key;
    private final P preValue;

    PushOp(final K key, final P preValue) {
      this.key = key;
      this.preValue = preValue;
    }

    /**
     * Read from kvStore, modify (update), and write to kvStore.
     */
    @Override
    public void apply(final MemoryStore<K> memoryStore) {
      try {
        final Pair<K, V> oldKVPair = memoryStore.get(DATA_TYPE, key);

        final V oldValue;
        if (null == oldKVPair) {
          LOG.log(Level.FINE, "The value did not exist. Will use the initial value specified in ParameterUpdater.");
          oldValue = parameterUpdater.initValue(key);
        } else {
          oldValue = oldKVPair.getSecond();
        }

        final V deltaValue = parameterUpdater.process(key, preValue);
        if (deltaValue == null) {
          return;
        }

        final V updatedValue = parameterUpdater.update(oldValue, deltaValue);
        memoryStore.put(DATA_TYPE, key, updatedValue);
      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Exception occurred", e);
      }
    }
  }

  /**
   * A pull operation.
   */
  private class PullOp implements Op<K, V> {
    private final K key;
    private final String srcId;

    PullOp(final K key, final String srcId) {
      this.key = key;
      this.srcId = srcId;
    }

    /**
     * Read from kvStore and send the key-value pair to srcId.
     * To ensure atomicity, the key-value pair should be serialized immediately in sender.
     */
    @Override
    public void apply(final MemoryStore<K> memoryStore) {
      try {
        final Pair<K, V> kvPair = memoryStore.get(DATA_TYPE, key);
        final V value;
        if (null == kvPair) {
          final Pair<K, Boolean> result = memoryStore.put(DATA_TYPE, key, parameterUpdater.initValue(key));
          final boolean isSuccess = result.getSecond();
          if (!isSuccess) {
            throw new RuntimeException("The data does not exist. Tried to put the initial value, but has failed");
          }
          final Pair<K, V> initializedPair = memoryStore.get(DATA_TYPE, key);
          value = initializedPair.getSecond();
        } else {
          value = kvPair.getSecond();
        }
        sender.sendReplyMsg(srcId, key, value);

      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Exception occurred", e);
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
    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.
    private final MemoryStore<K> memoryStore;

    private volatile boolean shutdown = false;

    ServerThread(final int queueSize, final MemoryStore<K> memoryStore) {
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
          op.apply(memoryStore);
        } catch (final InterruptedException e) {
          LOG.log(Level.FINEST, "Poll failed with InterruptedException", e);
          continue;
        }

        // Then, drain up to LOCAL_OPS_SIZE of the remaining queue and apply.
        // Calling drainTo does not block if queue is empty, which is why we poll first.
        // This should be faster than polling each op, because the blocking queue's lock is only acquired once.
        queue.drainTo(localOps, drainSize);
        for (final Op<K, V> op : localOps) {
          op.apply(memoryStore);
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
   * Assigns one thread per block.
   * The first implementation allocates unseen block to threads in a round-robin fashion.
   * Note that the load is not perfectly distributed evenly, because the blocks that have moved out are not considered.
   */
  private static class ThreadResolver {
    // Naive version: round-robin
    private final int numThreads;
    private AtomicInteger nextIndex = new AtomicInteger(0);
    private Map<Integer, Integer> blockToThread = new HashMap<>();

    ThreadResolver(final int numThreads) {
      this.numThreads = numThreads;
    }

    synchronized int resolveThread(final int blockId) {
      final Integer threadId = blockToThread.get(blockId);
      if (null == threadId) {
        int index = nextIndex.getAndIncrement() % numThreads;
        blockToThread.put(blockId, index);
        LOG.log(Level.FINEST, "BlockId {0} / ThreadId {1}", new Object[] {blockId, index});
        return index;
      } else {
        return threadId;
      }
    }
  }
}
