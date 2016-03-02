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

import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.common.partitioned.resolver.ServerResolver;
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
 * A Partitioned Parameter Server.
 * Receives push and pull operations from (e.g., from the network) and immediately queues them.
 * The processing loop (for each partition) applies these operations in order; for pull operations
 * this results in a send call via {@link PartitionedServerSideReplySender}.
 * For more information about the partition implementation, see {@link Partition}.
 *
 * Supports a static number of partitions (the number of partitions is fixed at construction time).
 */
@EvaluatorSide
public final class PartitionedParameterServer<K, P, V> {
  private static final Logger LOG = Logger.getLogger(PartitionedParameterServer.class.getName());

  /**
   * Network Connection Service endpoint of this ParameterServer.
   */
  private final String endpointId;

  /**
   * ServerResolver that maps hashed keys to partitions.
   */
  private final ServerResolver serverResolver;

  /**
   * Max size of each partition's queue.
   */
  private final int queueSize;

  /**
   * Thread pool, where each Partition is submitted.
   */
  private final ExecutorService threadPool;

  /**
   * Running partitions.
   */
  private final Map<Integer, Partition<K, V>> partitions;

  /**
   * Object for processing preValues and applying updates to existing values.
   */
  private final ParameterUpdater<K, P, V> parameterUpdater;

  /**
   * Sender that sends pull responses.
   */
  private final PartitionedServerSideReplySender<K, V> sender;

  @Inject
  private PartitionedParameterServer(@Parameter(EndpointId.class) final String endpointId,
                                     @Parameter(ServerQueueSize.class) final int queueSize,
                                     final ServerResolver serverResolver,
                                     final ParameterUpdater<K, P, V> parameterUpdater,
                                     final PartitionedServerSideReplySender<K, V> sender) {
    this.endpointId = endpointId;
    this.serverResolver = serverResolver;
    this.queueSize = queueSize;
    this.threadPool = Executors.newFixedThreadPool(serverResolver.getPartitions(endpointId).size());
    this.partitions = initPartitions();
    this.parameterUpdater = parameterUpdater;
    this.sender = sender;
  }

  /**
   * Call after initializing numPartitions and threadPool.
   */
  private Map<Integer, Partition<K, V>> initPartitions() {
    final Map<Integer, Partition<K, V>> initialized = new HashMap<>();
    final List<Integer> localPartitions = serverResolver.getPartitions(endpointId);
    LOG.log(Level.INFO, "Initializing {0} partitions", localPartitions.size());
    for (final int partitionIndex : localPartitions) {
      final Partition<K, V> partition = new Partition<>(queueSize);
      initialized.put(partitionIndex, partition);
      threadPool.submit(partition);
    }
    return initialized;
  }

  /**
   * Process a {@code preValue} sent from a worker and store the resulting value.
   * Uses {@link ParameterUpdater} to generate a value from {@code preValue} and to apply the generated value to
   * the k-v store.
   *
   * The push operation is enqueued to its partition and returned immediately.
   *
   * @param key key object that {@code preValue} is associated with
   * @param preValue preValue sent from the worker
   * @param keyHash hash of the key, a positive integer used to map to the correct partition
   */
  public void push(final K key, final P preValue, final int keyHash) {
    partitions.get(serverResolver.resolvePartition(keyHash)).enqueue(new PushOp(key, preValue));
  }

  /**
   * Reply to srcId via {@link PartitionedServerSideReplySender}
   * with the value corresponding to the key.
   *
   * The pull operation is enqueued to its partition and returned immediately.
   *
   * @param key key object that the requested {@code value} is associated with
   * @param srcId network Id of the requester
   * @param keyHash hash of the key, a positive integer used to map to the correct partition
   */
  public void pull(final K key, final String srcId, final int keyHash) {
    partitions.get(serverResolver.resolvePartition(keyHash)).enqueue(new PullOp(key, srcId));
  }

  /**
   * @return number of operations pending, on all queues
   */
  public int opsPending() {
    int sum = 0;
    for (final Partition<K, V> partition : partitions.values()) {
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

    public PushOp(final K key, final P preValue) {
      this.key = key;
      this.preValue = preValue;
    }

    /**
     * Read from kvStore, modify (update), and write to kvStore.
     */
    @Override
    public void apply(final Map<K, V> kvStore) {
      if (!kvStore.containsKey(key)) {
        kvStore.put(key, parameterUpdater.initValue(key));
      }

      final V deltaValue = parameterUpdater.process(key, preValue);
      if (deltaValue == null) {
        return;
      }

      final V updatedValue = parameterUpdater.update(kvStore.get(key), deltaValue);
      kvStore.put(key, updatedValue);
    }
  }

  /**
   * A pull operation.
   */
  private class PullOp implements Op<K, V> {
    private final K key;
    private final String srcId;

    public PullOp(final K key, final String srcId) {
      this.key = key;
      this.srcId = srcId;
    }

    /**
     * Read from kvStore and send the key-value pair to srcId.
     * To ensure atomicity, the key-value pair should be serialized immediately in sender.
     */
    @Override
    public void apply(final Map<K, V> kvStore) {
      if (!kvStore.containsKey(key)) {
        kvStore.put(key, parameterUpdater.initValue(key));
      }

      sender.sendReplyMsg(srcId, key, kvStore.get(key));
    }
  }

  /**
   * A partition of the parameter server. Must be started as a thread.
   * All push and pull operations should be sent to the appropriate partition.
   * The partition's processing loop dequeues and applies operations to its local kvStore.
   * The queue and single thread per partition ensures that all operations on a key
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
  private static class Partition<K, V> implements Runnable {
    private static final long QUEUE_TIMEOUT_MS = 3000;

    private final Map<K, V> kvStore;
    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.

    private volatile boolean shutdown = false;

    public Partition(final int queueSize) {
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
