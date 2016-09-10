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
import edu.snu.cay.dolphin.async.metric.avro.ParameterWorkerMetrics;
import edu.snu.cay.services.ps.PSParameters.KeyCodecName;
import edu.snu.cay.services.ps.common.Statistics;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.worker.api.*;
import edu.snu.cay.services.ps.worker.parameters.*;
import edu.snu.cay.utils.StateMachine;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.ps.worker.parameters.PullRetryTimeoutMs.TIMEOUT_NO_RETRY;

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
   * The maximum number of pending pulls allowed for each WorkerThread.
   */
  private final int maxPendingPullsPerThread;

  /**
   * Resolve to a server's Network Connection Service identifier based on hashed key.
   */
  private final ServerResolver serverResolver;

  /**
   * Number of WorkerThreads.
   */
  private final int numWorkerThreads;

  /**
   * Thread pool, where each thread is submitted.
   */
  private final ExecutorService threadPool;

  /**
   * Worker threads that process push & pull operations by sending messages to its corresponding server.
   */
  private final WorkerThread[] workerThreads;

  /**
   * A thread that retries the pull request by enqueueing an operation for retry to its corresponding queue.
   */
  private final RetryThread retryThread;

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

  private final Statistics[] pullStats;
  private final Statistics[] pushStats;
  private final Statistics[] networkStats;
  private final Statistics[] waitingStats;
  private final Statistics[] sentBytesStats;
  private final Statistics[] receivedBytesStats;
  private final Ticker ticker = Ticker.systemTicker();

  @Inject
  private AsyncParameterWorker(@Parameter(ParameterWorkerNumThreads.class) final int numWorkerThreads,
                               @Parameter(WorkerQueueSize.class) final int queueSize,
                               @Parameter(PullRetryTimeoutMs.class) final long pullRetryTimeoutMs,
                               @Parameter(MaxPendingPullsPerThread.class) final int maxPendingPullsPerThread,
                               @Parameter(WorkerKeyCacheSize.class) final int keyCacheSize,
                               @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                               final ServerResolver serverResolver,
                               final InjectionFuture<WorkerMsgSender<K, P>> sender) {
    this.numWorkerThreads = numWorkerThreads;
    this.maxPendingPullsPerThread = maxPendingPullsPerThread;
    this.serverResolver = serverResolver;
    this.sender = sender;
    this.pullStats = Statistics.newInstances(numWorkerThreads);
    this.pushStats = Statistics.newInstances(numWorkerThreads);
    this.networkStats = Statistics.newInstances(numWorkerThreads);
    this.waitingStats = Statistics.newInstances(numWorkerThreads);
    this.sentBytesStats = Statistics.newInstances(numWorkerThreads);
    this.receivedBytesStats = Statistics.newInstances(numWorkerThreads);
    // numWorkerThreads + 1 for retry thread
    this.threadPool = Executors.newFixedThreadPool(numWorkerThreads + 1);
    this.workerThreads = initWorkerThreads(queueSize);
    this.retryThread = new RetryThread(pullRetryTimeoutMs);
    this.threadPool.submit(retryThread);
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
   * Call after initializing threadPool.
   */
  @SuppressWarnings("unchecked")
  private WorkerThread[] initWorkerThreads(final int queueSize) {
    LOG.log(Level.INFO, "Initializing {0} PW threads", numWorkerThreads);
    final WorkerThread[] initialized = (WorkerThread[]) Array.newInstance(WorkerThread.class, numWorkerThreads);
    for (int i = 0; i < numWorkerThreads; i++) {
      initialized[i] = new WorkerThread(queueSize, pullStats[i], pushStats[i], networkStats[i], waitingStats[i],
          sentBytesStats[i], receivedBytesStats[i]);
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
      throw new RuntimeException("Exception while loading encoded key from cache", e);
    }
  }

  private void push(final EncodedKey<K> encodedKey, final P preValue) {
    final int threadId = getThreadIndex(encodedKey.getHash());
    final PushOp pushOp = new PushOp(workerThreads[threadId], encodedKey, preValue);
    workerThreads[threadId].enqueue(pushOp);
  }

  @Override
  public V pull(final K key) {
    try {
      return pull(encodedKeyCache.get(key));
    } catch (final ExecutionException e) {
      throw new RuntimeException("Exception while loading encoded key from cache", e);
    }
  }

  private V pull(final EncodedKey<K> encodedKey) {
    final int threadId = getThreadIndex(encodedKey.getHash());
    final PullOp pullOp = new PullOp(workerThreads[threadId], encodedKey);
    workerThreads[threadId].enqueue(pullOp);
    return pullOp.getResult();
  }

  @Override
  public List<V> pull(final List<K> keys) {
    // transform keys to encoded keys
    final List<EncodedKey<K>> encodedKeys = new ArrayList<>(keys.size());
    for (final K key : keys) {
      try {
        encodedKeys.add(encodedKeyCache.get(key));
      } catch (final ExecutionException e) {
        throw new RuntimeException("Exception while loading encoded key from cache", e);
      }
    }

    return pullEncodedKeys(encodedKeys);
  }

  private List<V> pullEncodedKeys(final List<EncodedKey<K>> encodedKeys) {
    final List<PullOp> pullOps = new ArrayList<>(encodedKeys.size());
    for (final EncodedKey<K> encodedKey : encodedKeys) {
      final int threadId = getThreadIndex(encodedKey.getHash());
      final PullOp pullOp = new PullOp(workerThreads[threadId], encodedKey);
      pullOps.add(pullOp);
      workerThreads[threadId].enqueue(pullOp);
    }
    final List<V> values = new ArrayList<>(pullOps.size());
    for (final PullOp pullOp : pullOps) {
      values.add(pullOp.getResult());
    }
    return values;
  }

  public void invalidateAll() {
    // do nothing, since current code does not maintain model cache
    // TODO #803: Current codebase does not maintain local cache. We should implement this later for higher performance.
  }

  private int getThreadIndex(final int keyHash) {
    return keyHash % numWorkerThreads;
  }

  /**
   * Close the worker, after waiting a maximum of {@code timeoutMs} milliseconds
   * for queued messages to be sent.
   */
  @Override
  public void close(final long timeoutMs) throws InterruptedException, TimeoutException, ExecutionException {

    final Future result = Executors.newSingleThreadExecutor().submit(() -> {
        // Close all threads
        for (int i = 0; i < numWorkerThreads; i++) {
          workerThreads[i].startClose();
        }
        retryThread.startClose();

        // Wait for close to complete on all threads
        for (int i = 0; i < numWorkerThreads; i++) {
          workerThreads[i].waitForClose();
        }
        retryThread.waitForClose();
      }
    );

    result.get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Handles incoming pull replies, by setting the value of pull ops.
   * See {@link PullRequest#completePendingOps(Object, long, int)}.
   * WorkerThread will be notified if the thread is waiting for completion of this pull request in case
   * 1) when push for the same key is requested or
   * 2) when the number of pending pulls on the thread has reached the limit (maxPendingPullsPerThread).
   */
  @Override
  public void processPullReply(final K key, final V value, final int requestId, final long elapsedTimeInServer,
                               final int numReceivedBytes) {
    final EncodedKey<K> encodedKey;
    try {
      encodedKey = encodedKeyCache.get(key);
    } catch (final ExecutionException e) {
      throw new RuntimeException("Exception while loading encoded key from cache", e);
    }
    final WorkerThread workerThread = workerThreads[getThreadIndex(encodedKey.getHash())];

    synchronized (workerThread) {
      final Map<K, PullRequest> pendingPullRequests = workerThread.pendingPullRequests;
      final PullRequest pullRequest = pendingPullRequests.get(key);
      if (pendingPullRequestExists(pullRequest, requestId)) {
        pendingPullRequests.remove(key).completePendingOps(value, elapsedTimeInServer, numReceivedBytes);
        // if number of pendingPullRequests becomes maxPendingPullsPerThread - 1 by this reply,
        // should notify waiting WorkerThread (may not exist) which wants to make another pullRequest
        if (pendingPullRequests.size() == maxPendingPullsPerThread - 1) {
          workerThread.notify();
        }
      } else {
        LOG.log(Level.INFO, "Could not find corresponding pullRequest for key: {0}, requestId: {1}",
            new Object[]{key, requestId});
      }
    }
  }

  /**
   * Handles incoming pull rejects, by retrying pull request.
   * See {@link PullRequest#reject()}.
   * This will interrupt the WorkerThread after enqueueing {@link PullOp}, to handle the retry op first.
   */
  @Override
  public void processPullReject(final K key, final int requestId) {
    final EncodedKey<K> encodedKey;
    try {
      encodedKey = encodedKeyCache.get(key);
    } catch (final ExecutionException e) {
      throw new RuntimeException("Exception while loading encoded key from cache", e);
    }
    final WorkerThread workerThread = workerThreads[getThreadIndex(encodedKey.getHash())];

    synchronized (workerThread) {
      final Map<K, PullRequest> pendingPullRequests = workerThread.pendingPullRequests;
      final PullRequest pullRequest = pendingPullRequests.get(key);
      if (pendingPullRequestExists(pullRequest, requestId)) {
        pullRequest.reject();
      } else {
        LOG.log(Level.INFO, "Could not find corresponding pullRequest for key: {0}, requestId: {1}",
            new Object[]{key, requestId});
      }
    }
  }

  /**
   * Check existence of the pendingPullRequest corresponding to received pullReply or pullReject.
   * @param pullRequest a pullRequest object associated with the received key, may be null if not exists
   * @param requestId received pull request id
   * @return true if the corresponding pullRequest exists, otherwise false
   */
  private boolean pendingPullRequestExists(final PullRequest pullRequest, final int requestId) {
    if (pullRequest == null) {
      // Because we assign each key to a dedicated thread, there can be at most one active pullRequest for a key.
      // But occasionally, multiple responses for a single pullRequest may arrive
      // if the worker retried due to the late response from the target server.
      LOG.log(Level.FINE, "Pending pull was not found");
      return false;
    } else if (requestId != pullRequest.getRequestId()) {
      // Although there is a pullRequest for this key, the request id may be different from each other.
      // We can filter out pull replies from retries of previous requests.
      LOG.log(Level.FINE, "Pull request id not matched, received: {0}, actual: {1}",
          new Object[]{requestId, pullRequest.getRequestId()});
      return false;
    } else {
      return true;
    }
  }

  /**
   * Handles incoming push rejects, by retrying push request.
   * This will interrupt the WorkerThread after enqueueing {@link PushOp}, to handle the retry op first.
   */
  @Override
  public void processPushReject(final K key, final P preValue) {
    final EncodedKey<K> encodedKey;
    try {
      encodedKey = encodedKeyCache.get(key);
    } catch (final ExecutionException e) {
      throw new RuntimeException("Exception while loading encoded key from cache", e);
    }
    final WorkerThread workerThread = workerThreads[getThreadIndex(encodedKey.getHash())];
    workerThread.enqueueRetryOp(new PushOp(workerThread, encodedKey, preValue));
    workerThread.interruptToTriggerRetry();
  }

  /**
   * Sends a push msg for the {@code encodedKey} to the target server.
   * To send push message atomically, do not respond to WorkerThread interrupt.
   * @param encodedKey encoded key
   * @param preValue preValue
   */
  private int sendPushMsg(final EncodedKey<K> encodedKey, final P preValue) {
    int resendCount = 0;
    int numSentBytes;
    boolean interrupted = false;
    while (true) {
      if (resendCount++ > MAX_RESEND_COUNT) {
        throw new RuntimeException("Fail to send a push message");
      }

      // Re-resolve server for every retry, because msg sender throws NetworkException
      // when routing table is obsolete and indicates non-existing server.
      final String serverId = serverResolver.resolveServer(encodedKey.getHash());
      LOG.log(Level.FINEST, "Resolve server for encodedKey. key: {0}, hash: {1}, serverId: {2}",
          new Object[]{encodedKey.getKey(), encodedKey.getHash(), serverId});

      try {
        numSentBytes = sender.get().sendPushMsg(serverId, encodedKey, preValue);
        break;
      } catch (final NetworkException e) {
        LOG.log(Level.WARNING, "NetworkException while sending push msg. Do retry", e);
      }

      LOG.log(Level.WARNING, "Wait {0} ms before resending a push msg", RESEND_INTERVAL_MS);
      try {
        // may not sleep for RESEND_INTERVAL_MS due to interrupt
        Thread.sleep(RESEND_INTERVAL_MS);
      } catch (final InterruptedException e) {
        interrupted = true;
        LOG.log(Level.FINEST, "Interrupted while waiting for routing table to be updated", e);
      }
    }

    // restore thread interrupt state
    if (interrupted) {
      Thread.currentThread().interrupt();
    }

    return numSentBytes;
  }

  /**
   * Sends a pull msg for the {@code encodedKey} to the target server.
   * To send push message atomically, do not respond to WorkerThread interrupt.
   * @param encodedKey encoded key
   * @param requestId pull request id
   */
  private void sendPullMsg(final EncodedKey<K> encodedKey, final int requestId) {
    int resendCount = 0;
    boolean interrupted = false;
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
        sender.get().sendPullMsg(serverId, encodedKey, requestId);
        break;
      } catch (final NetworkException e) {
        LOG.log(Level.WARNING, "NetworkException while sending pull msg. Do retry", e);
      }

      LOG.log(Level.WARNING, "Wait {0} ms before resending a pull msg", RESEND_INTERVAL_MS);
      try {
        // may not sleep for RESEND_INTERVAL_MS due to interrupt
        Thread.sleep(RESEND_INTERVAL_MS);
      } catch (final InterruptedException e) {
        interrupted = true;
        LOG.log(Level.FINEST, "Interrupted while waiting for routing table to be updated", e);
      }
    }
    // restore thread interrupt state
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ParameterWorkerMetrics buildParameterWorkerMetrics() {
    final Pair<Integer, Double> totalPullStat = summarizeAndResetStats(pullStats);
    final Pair<Integer, Double> totalPushStat = summarizeAndResetStats(pushStats);
    final Pair<Integer, Double> totalNetworkStat = summarizeAndResetStats(networkStats);
    final Pair<Integer, Double> totalWaitingStat = summarizeAndResetStats(waitingStats);
    final Pair<Integer, Double> totalSentBytesStat = summarizeAndResetStats(sentBytesStats);
    final Pair<Integer, Double> totalReceivedBytesStat = summarizeAndResetStats(receivedBytesStats);

    final Double receivedBytes = totalReceivedBytesStat.getRight();
    final Double sentBytes = totalSentBytesStat.getRight();
    LOG.log(Level.SEVERE, "Received {0} / Sent {1}", new Object[] {receivedBytes, sentBytes});
    return ParameterWorkerMetrics.newBuilder()
        .setNumThreads(numWorkerThreads)
        .setTotalPullCount(totalPullStat.getLeft())
        .setTotalPullTime(totalPullStat.getRight())
        .setTotalPushCount(totalPushStat.getLeft())
        .setTotalPushTime(totalPushStat.getRight())
        .setTotalNetworkStatCount(totalNetworkStat.getLeft())
        .setTotalNetworkTime(totalNetworkStat.getRight())
        .setTotalWaitingStatCount(totalWaitingStat.getLeft())
        .setTotalWaitingTime(totalWaitingStat.getRight())
        .setTotalSentBytes(totalSentBytesStat.getRight().intValue())
        .setTotalSentBytesStatCount(totalSentBytesStat.getLeft())
        .setTotalReceivedBytes(totalReceivedBytesStat.getRight().intValue())
        .setTotalReceivedBytesStatCount(totalReceivedBytesStat.getLeft())
        .build();
  }

  /**
   * Computes the total number and time spent on processing requests with the WorkerThreads in this worker.
   * Summarizes stats across all WorkerThreads in this worker and resets them.
   */
  private Pair<Integer, Double> summarizeAndResetStats(final Statistics[] stats) {
    int processedCount = 0;
    double procTimeSum = 0D;

    for (final Statistics stat : stats) {
      processedCount += stat.count();
      procTimeSum += stat.sum();
      stat.reset();
    }

    return Pair.of(processedCount, procTimeSum);
  }

  /**
   * A generic operation requested from parameter worker clients.
   * Operations are queued at each WorkerThread.
   */
  private interface Op {

    /**
     * Method to apply when dequeued by the WorkerThread.
     * May wait until this operation is ready to be executed.
     * When it is interrupted, the status of this operation should be same as before calling this method.
     * @throws InterruptedException when the executing thread is interrupted
     */
    void apply() throws InterruptedException;

    /**
     * Method to retry operation when dequeued by the WorkerThread.
     * This method can not be interrupted.
     */
    void retry();
  }

  /**
   * A push operation.
   * Should wait if there is a pullRequest for the same key.
   */
  private final class PushOp implements Op {
    private final WorkerThread workerThread;
    private final EncodedKey<K> encodedKey;
    private final P preValue;

    private final long enqueueTime;

    PushOp(final WorkerThread workerThread, final EncodedKey<K> encodedKey, final P preValue) {
      this.workerThread = workerThread;
      this.encodedKey = encodedKey;
      this.preValue = preValue;

      this.enqueueTime = ticker.read();
    }

    @Override
    public void apply() throws InterruptedException {
      synchronized (workerThread) {
        final Map<K, PullRequest> pendingPullRequests = workerThread.pendingPullRequests;
        while (pendingPullRequests.containsKey(encodedKey.getKey())) {
          final PullRequest pullRequest = pendingPullRequests.get(encodedKey.getKey());
          LOG.log(Level.WARNING,
              "PushOp is waiting for completion of previous pull request for the same key: {0}, pullRequestId: {1}",
              new Object[]{encodedKey.getKey(), pullRequest.getRequestId()});
          pullRequest.waitForComplete();
        }

        final long pushStartTime = ticker.read();
        workerThread.waitingStat.put(pushStartTime - enqueueTime);

        final int numSentBytes = sendPushMsg(encodedKey, preValue);
        workerThread.sentBytesStat.put(numSentBytes);

        final long endTime = ticker.read();
        workerThread.pushStat.put(endTime - pushStartTime);
      }
    }

    /**
     * Simply re-send rejected push message.
     */
    @Override
    public void retry() {
      // TODO #803: Once we implement worker-side cache, we can guarantee read-my-update even with retrying push.
      synchronized (workerThread) {
        final int numSentBytes = sendPushMsg(encodedKey, preValue);
        LOG.log(Level.SEVERE, "Sent {0} bytes", numSentBytes);
        workerThread.sentBytesStat.put(numSentBytes);
      }
    }
  }

  /**
   * A pull operation.
   * Should wait if the number of pendingPullRequests on this thread exceeds {@link #maxPendingPullsPerThread}.
   * Also exposes a blocking {@link #getResult()} method to retrieve the result of the pull.
   */
  private final class PullOp implements Op {
    private final WorkerThread workerThread;
    private final EncodedKey<K> encodedKey;
    private V value;
    private int requestId;

    private final long enqueueTime;
    private long pullStartTime;

    PullOp(final WorkerThread workerThread, final EncodedKey<K> encodedKey) {
      this.workerThread = workerThread;
      this.encodedKey = encodedKey;

      this.enqueueTime = ticker.read();
    }

    @Override
    public void apply() throws InterruptedException {
      synchronized (workerThread) {
        final Map<K, PullRequest> pendingPullRequests = workerThread.pendingPullRequests;
        PullRequest pullRequest = pendingPullRequests.get(encodedKey.getKey());

        // send a new pull request if no request for the key has been made yet
        if (pullRequest == null) {
          while (pendingPullRequests.size() >= maxPendingPullsPerThread) {
            workerThread.wait();
          }

          this.pullStartTime = ticker.read();
          workerThread.waitingStat.put(pullStartTime - enqueueTime);

          pullRequest = new PullRequest(workerThread, encodedKey, this);
          requestId = pullRequest.getRequestId();
          pendingPullRequests.put(encodedKey.getKey(), pullRequest);
          sendPullMsg(encodedKey, requestId);

          // wait with other operations that were previously requested, if the pull request has been sent already
        } else {
          this.pullStartTime = ticker.read();
          workerThread.waitingStat.put(pullStartTime - enqueueTime);

          pullRequest.addPendingOp(this);
        }
      }
    }

    /**
     * Re-send message for pull request, if the corresponding request exists.
     */
    @Override
    public void retry() {
      synchronized (workerThread) {
        final Map<K, PullRequest> pendingPullRequests = workerThread.pendingPullRequests;
        final PullRequest pullRequest = pendingPullRequests.get(encodedKey.getKey());
        if (pendingPullRequestExists(pullRequest, requestId)) {
          pullRequest.processRetry();
        } else {
          LOG.log(Level.INFO, "Could not find corresponding pullRequest for key: {0}, requestId: {1}",
              new Object[]{encodedKey.getKey(), requestId});
        }
      }
    }

    /**
     * A blocking get.
     * @return the value
     */
    public synchronized V getResult() {
      while (value == null) {
        try {
          wait();
        } catch (final InterruptedException e) {
          // Need to decide policy for dealing with client thread interrupt
          LOG.log(Level.WARNING, "InterruptedException on wait", e);
        }
      }
      return value;
    }

    public synchronized void setResult(final V newValue) {
      final long endTime = ticker.read();
      workerThread.pullStat.put(endTime - pullStartTime);

      this.value = newValue;
      notify();
    }
  }

  /**
   * An object for managing pending pull request.
   * Multiple invocations of {@link ParameterWorker#pull(Object)} for the same key should not instantiate
   * multiple {@link PullRequest} objects, unless preceding request is resolved by receiving proper reply.
   * Instead, {@link #addPendingOp(PullOp)} will be used to notify all clients of operations
   * without making multiple {@link PullRequest} objects and sending more than one pull requests to server.
   */
  private final class PullRequest {

    /*
     * A pull request is in one of the following three states:
     *
     * 1) INIT: When a pull request is created initially, or message for retrying a pull request has been sent.
     *
     * 2) NEED_RETRY: When response for a request has not arrived until the upcoming timeout-checking,
     * the pull request is marked as NEED_RETRY.
     * RetryThread will later(in the next timeout-checking) enqueues pull requests
     * in this state to the RetryQueue of the corresponding WorkerThread.
     *
     * 3) RETRY_REQUESTED: When a pull request is retried (enqueued in RetryQueue precisely),
     *   (a) when RetryThread enqueues pull requests in NEED_RETRY state, or
     *   (b) when WorkerThread enqueues the rejected pull requests.
     */
    private static final int INIT_STATE = 0;
    private static final int NEED_RETRY_STATE = 1;
    private static final int RETRY_REQUESTED_STATE = 2;

    private final WorkerThread workerThread;
    private final EncodedKey<K> encodedKey;
    private final ArrayList<PullOp> pendingOps;

    private final int requestId;
    private int retryCount;
    private int state; // accessors are synchronized
    private boolean waiting;
    private final long pullStartTime;

    PullRequest(final WorkerThread workerThread, final EncodedKey<K> encodedKey, final PullOp pullOp) {
      this.workerThread = workerThread;
      this.encodedKey = encodedKey;
      this.pendingOps = new ArrayList<>();
      this.pendingOps.add(pullOp);

      this.requestId = workerThread.getNewRequestId();
      this.retryCount = 0;
      this.state = INIT_STATE;
      this.waiting = false;
      this.pullStartTime = ticker.read();
    }

    /**
     * Retrieves pull request id.
     * All {@link PullRequest} objects for the same key from one {@link ParameterWorker}
     * should not have the same requestId.
     * {@link #pendingPullRequestExists(PullRequest, int)}} uses this id to determine
     * whether the received pull reply(or reject) is the one requested for.
     * @return pull request id
     */
    int getRequestId() {
      return requestId;
    }

    /**
     * Process pull retry by sending pull message once again, only if the state is {@link #RETRY_REQUESTED_STATE}.
     * After sending the request message for retry, the state of this pull request is reset to {@link #INIT_STATE},
     * and the retry count is incremented.
     * @throws RuntimeException if the retrial count exceeded maximum number of retries
     */
    synchronized void processRetry() {
      if (retryCount++ >= MAX_PULL_RETRY_COUNT) {
        throw new RuntimeException("Fail to load a value for pull");
      }

      LOG.log(Level.WARNING, "Retry pull request for key {0}. This is {1}-th retry",
          new Object[]{encodedKey.getKey(), retryCount});
      sendPullMsg(encodedKey, requestId);
      state = INIT_STATE;
    }

    /**
     * Request retry of this pull request if the timeout exceeded.
     * We do not guarantee a strict timeout, as we check timeout and send pull retry message asynchronously.
     * @return true if timeout exceeded
     */
    synchronized boolean requestRetryIfTimeout() {
      switch (state) {
      case INIT_STATE:
        state = NEED_RETRY_STATE;
        return false;
      case NEED_RETRY_STATE:
        LOG.log(Level.INFO, "Pull request time out for key: {0}, requestId: {1}, retryCount: {2}",
            new Object[]{encodedKey.getKey(), requestId, retryCount});
        state = RETRY_REQUESTED_STATE;
        workerThread.enqueueRetryOp(pendingOps.get(0));
        return true;
      case RETRY_REQUESTED_STATE:
        return false;
      default:
        throw new RuntimeException("Unknown state of pull request");
      }
    }

    /**
     * Add a pull operation to be resolved when pull reply arrives.
     * @param pullOp a pending pull operation
     */
    void addPendingOp(final PullOp pullOp) {
      synchronized (workerThread) {
        pendingOps.add(pullOp);
      }
    }

    /**
     * Complete pending pull operations in {@link #pendingOps}
     * by notifying client threads using {@link PullOp#setResult(Object)}.
     * Also, notify WorkerThread if it is waiting for completion of this pull request
     * to process push operation for the same key.
     * @param value value received from server
     * @param elapsedTimeInServer elapsed time since pull request's arrival at server
     * @param numReceivedBytes the number of total messages in bytes.
     */
    void completePendingOps(final V value, final long elapsedTimeInServer, final int numReceivedBytes) {
      synchronized (workerThread) {

        final long pullTime = ticker.read() - pullStartTime;
        workerThread.networkStat.put(pullTime - elapsedTimeInServer);
        LOG.log(Level.SEVERE, "Received {0} bytes", numReceivedBytes);
        workerThread.receivedBytesStat.put(numReceivedBytes);

        for (final PullOp pullOp : pendingOps) {
          pullOp.setResult(value);
        }
        if (waiting) {
          workerThread.notify();
        }
      }
    }

    /**
     * Reject this pull request, and request for retry.
     */
    synchronized void reject() {
      // if state is RETRY_REQUESTED, retry was already requested due to timeout or previous reject, so skip this turn
      if (state != RETRY_REQUESTED_STATE) {
        state = RETRY_REQUESTED_STATE;
        workerThread.enqueueRetryOp(pendingOps.get(0));
        workerThread.interruptToTriggerRetry();
      }
    }

    /**
     * Wait until this pull request is completed.
     * Should precede {@link #completePendingOps(Object, long, int)}. If not, the waiting thread will be never awoken.
     * @throws InterruptedException when the executing thread is interrupted
     */
    void waitForComplete() throws InterruptedException {
      synchronized (workerThread) {
        waiting = true;
        workerThread.wait();
      }
    }
  }

  /**
   * A thread abstraction for parallelizing worker's accesses to keys, by partitioning key space.
   * See {@link #getThreadIndex(int)}.
   * The basic structure is similar to the partition for the Server at
   * {@link edu.snu.cay.services.ps.server.api.ParameterServer}.
   *
   * The threads at the Worker can be independent of the partitions at the Server. In other words,
   * the number of worker-side threads does not have to be equal to the number of server-side partitions.
   */
  // TODO #803: Current codebase does not maintain local cache. We should implement this later for higher performance.
  private final class WorkerThread implements Runnable {
    private static final long QUEUE_TIMEOUT_MS = 3000;
    private static final String STATE_RUNNING = "RUNNING";
    private static final String STATE_CLOSING = "CLOSING";
    private static final String STATE_CLOSED = "CLOSED";

    private final Map<K, PullRequest> pendingPullRequests;
    private final BlockingQueue<Op> queue;
    private final BlockingQueue<Op> retryQueue;

    // Operations drained from the queue, and processed locally.
    private final ArrayList<Op> localOps;
    private final ArrayList<Op> localRetryOps;
    // Max number of operations to drain per iteration.
    private final int drainSize;

    private final StateMachine stateMachine;
    private Thread currentThread;
    private int requestId;

    private final Statistics pullStat;
    private final Statistics pushStat;
    private final Statistics networkStat;
    private final Statistics waitingStat;
    private final Statistics sentBytesStat;
    private final Statistics receivedBytesStat;

    WorkerThread(final int queueSize,
                 final Statistics pullStat,
                 final Statistics pushStat,
                 final Statistics networkStat,
                 final Statistics waitingStat,
                 final Statistics sentBytesStat,
                 final Statistics receivedBytesStat) {
      this.drainSize = queueSize / 10;
      this.pendingPullRequests = new ConcurrentHashMap<>();
      this.queue = new ArrayBlockingQueue<>(queueSize);
      this.retryQueue = new ArrayBlockingQueue<>(drainSize);
      this.localOps = new ArrayList<>(drainSize);
      this.localRetryOps = new ArrayList<>(drainSize);
      this.stateMachine = initStateMachine();
      this.requestId = 0;
      this.pullStat = pullStat;
      this.pushStat = pushStat;
      this.networkStat = networkStat;
      this.waitingStat = waitingStat;
      this.sentBytesStat = sentBytesStat;
      this.receivedBytesStat = receivedBytesStat;
    }

    private StateMachine initStateMachine() {
      return StateMachine.newBuilder()
          .addState(STATE_RUNNING, "PW thread is running. It executes operations in the queue.")
          .addState(STATE_CLOSING, "PW thread is closing. It will be closed after processing whole remaining ops.")
          .addState(STATE_CLOSED, "PW thread is closed. It finished processing whole remaining operations.")
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
    void enqueue(final Op op) {
      try {
        queue.put(op);
      } catch (final InterruptedException e) {
        // Need to decide policy for dealing with interrupts on client thread
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
      }
    }

    /**
     * Enqueue an retry operation onto the retry queue, blocking if the queue is full.
     * When the queue is full, this method will block; see {@link #enqueue(Op)}.
     * The retry queue has higher priority over the queue with normal ops.
     *
     * @param op the retry operation to enqueue
     */
    void enqueueRetryOp(final Op op) {
      try {
        retryQueue.put(op);
      } catch (final InterruptedException e) {
        // Need to decide policy for dealing with interrupts on NCS thread and retry thread
        LOG.log(Level.FINER, "Enqueue failed with InterruptedException. Try again", e);
      }
    }

    /**
     * Generate and return new request id for new {@link PullRequest}.
     * Because each WorkerThread handles disjoint key space, it is guaranteed that different {@link PullRequest}s
     * for the same key always have different request id.
     * @return new request id
     */
    int getNewRequestId() {
      return requestId++;
    }

    void interruptToTriggerRetry() {
      currentThread.interrupt();
    }

    /**
     * @return number of pending operations in the queue.
     */
    int opsPending() {
      return queue.size() + localOps.size();
    }

    private void processRetryOps() {
      if (!retryQueue.isEmpty()) {
        retryQueue.drainTo(localRetryOps, drainSize);
        for (final Op retryOp : localRetryOps) {
          retryOp.retry();
        }
        localRetryOps.clear();
      }
    }

    /**
     * Loop that dequeues operations and applies them.
     * Dequeues are only performed through this thread.
     */
    @Override
    public void run() {
      currentThread = Thread.currentThread();
      try {
        while (stateMachine.getCurrentState().equals(STATE_RUNNING) || !queue.isEmpty() || !retryQueue.isEmpty()) {
          // poll, the timeout allows the run thread to close cleanly within timeout ms.
          final Op op;
          try {
            op = queue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          } catch (final InterruptedException e) {
            processRetryOps();
            continue;
          }
          if (op == null) {
            continue;
          }

          // process polled op
          while (true) {
            try {
              op.apply();
              break;
            } catch (final InterruptedException e) {
              processRetryOps();
            }
          }

          // Then, drain up to drainSize of the remaining queue and apply.
          // Calling drainTo does not block if queue is empty, which is why we poll first.
          // This should be faster than polling each op, because the blocking queue's lock is only acquired once.
          queue.drainTo(localOps, drainSize);
          int i = 0;
          while (i < localOps.size()) {
            // for ops which do not check thread interrupt: check here manually and clear interrupt state
            if (Thread.interrupted()) {
              processRetryOps();
            }
            try {
              localOps.get(i).apply();
              i++;
            } catch (final InterruptedException e) {
              processRetryOps();
            }
          }
          localOps.clear();
        }

        finishClose();

        // catch and rethrow RuntimeException after leaving a log
        // otherwise, the thread disappears without any noticeable marks
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

  /**
   * A thread which checks timeout of remaining pendingPullRequests.
   * Traverse all remaining pendingPullRequests in {@link #workerThreads}
   * for every {@code pullRetryTimeoutMs} milliseconds and enqueue {@link PullOp} if necessary.
   * See {@link PullRequest#requestRetryIfTimeout()}.
   *
   * If {@code pullRetryTimeoutMs} is equal to {@link PullRetryTimeoutMs#TIMEOUT_NO_RETRY}, this thread do nothing.
   * In other words, there is no timeout mechanism.
   *
   * Timeout of pull request may happen because of the followings:
   * 1) minimum processing time for pull is longer than timeout
   * 2) server is overloaded
   * 3) pull request or reply message is missing due to network or other problems
   *
   * We should adjust timeout to be large enough to avoid 1) and not to worsen 2),
   * but small enough to quickly recover from 3).
   */
  private final class RetryThread implements Runnable {
    private static final String STATE_RUNNING = "RUNNING";
    private static final String STATE_CLOSING = "CLOSING";
    private static final String STATE_CLOSED = "CLOSED";

    private final StateMachine stateMachine;
    private final long pullRetryTimeoutMs;

    RetryThread(final long pullRetryTimeoutMs) {
      this.stateMachine = initStateMachine();
      this.pullRetryTimeoutMs = pullRetryTimeoutMs;
    }

    private StateMachine initStateMachine() {
      return StateMachine.newBuilder()
          .addState(STATE_RUNNING, "Retry thread is running." +
              "It checks pendingPullRequests and enqueues to proper retry queue if necessary.")
          .addState(STATE_CLOSING, "Retry thread is closing.")
          .addState(STATE_CLOSED, "Retry thread is closed.")
          .addTransition(STATE_RUNNING, STATE_CLOSING, "Time to close the thread.")
          .addTransition(STATE_CLOSING, STATE_CLOSED, "Closing the thread is done.")
          .setInitialState(STATE_RUNNING)
          .build();
    }

    @Override
    public void run() {
      if (pullRetryTimeoutMs == TIMEOUT_NO_RETRY) {
        return;
      }

      long elapsedTimeInMs = 0;

      while (stateMachine.getCurrentState().equals(STATE_RUNNING)) {
        // if previous scan took more than pullRetryTimeoutMs, do not sleep
        if (elapsedTimeInMs < pullRetryTimeoutMs) {
          try {
            // ensure that scan & retry do not occur with period smaller than pullRetryTimeoutMs
            Thread.sleep(pullRetryTimeoutMs - elapsedTimeInMs);
          } catch (final InterruptedException e) {
            LOG.log(Level.FINE, "Interrupt while sleeping for retry interval");
          }
        }

        final long startTime = ticker.read();

        // scan all pendingPullRequests in WorkerThreads, and check timeout
        for (final WorkerThread workerThread : workerThreads) {
          final Map<K, PullRequest> pendingPullRequests = workerThread.pendingPullRequests;

          // batching thread interrupt; interrupt the WorkerThread only once after scanning finishes
          boolean needInterrupt = false;
          for (final PullRequest pullRequest : pendingPullRequests.values()) {
            needInterrupt |= pullRequest.requestRetryIfTimeout();
          }
          if (needInterrupt) {
            workerThread.interruptToTriggerRetry();
          }
        }

        elapsedTimeInMs = TimeUnit.MILLISECONDS.convert(ticker.read() - startTime, TimeUnit.NANOSECONDS);
      }

      finishClose();
    }

    /**
     * Start closing the thread.
     * The thread will be closed after processing for all pending operations.
     */
    void startClose() {
      if (pullRetryTimeoutMs == TIMEOUT_NO_RETRY) {
        return;
      }
      stateMachine.setState(STATE_CLOSING);
    }

    /**
     * Notify that the thread is closed successfully.
     * It wakes up threads waiting in {@link #waitForClose()}.
     */
    private synchronized void finishClose() {
      if (pullRetryTimeoutMs == TIMEOUT_NO_RETRY) {
        return;
      }
      stateMachine.setState(STATE_CLOSED);
      notifyAll();
    }

    /**
     * Wait until thread is closed successfully.
     */
    synchronized void waitForClose() {
      if (pullRetryTimeoutMs == TIMEOUT_NO_RETRY) {
        return;
      }
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
