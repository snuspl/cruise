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

import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.exception.evaluator.NetworkException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Common test codes for both {@link AsyncParameterWorker} and {@link SSPParameterWorker}.
 */
final class ParameterWorkerTestUtil {
  static final long CLOSE_TIMEOUT = 5000;
  static final String MSG_THREADS_SHOULD_FINISH = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_THREADS_SHOULD_NOT_FINISH = "threads have finished but should not";
  private static final String MSG_RESULT_ASSERTION = "threads received incorrect values";

  private static final int NUM_PULL_HANDLING_THREADS = 2;
  static final long PULL_RETRY_TIMEOUT_MS = 1000;

  /**
   * Should not be instantiated.
   */
  private ParameterWorkerTestUtil() {
  }

  /**
   * Starts threads that process pull operations.
   * Worker enqueues pull operations in {@code keyToReplyQueue}, and processes reply messages that are assumed to be
   * responses for the worker's pull request.
   * @return an {@link ExecutorService} which the pull requests are processed with.
   */
  static ExecutorService startPullReplyingThreads(
      final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> keyToReplyQueue,
      final WorkerHandler<Integer, ?, Integer> workerHandler) {
    final Runnable pullReplyingThread = new Runnable() {
      @Override
      public void run() {
        while (true) {
          final Pair<EncodedKey<Integer>, Integer> request;
          try {
            request = keyToReplyQueue.take();
          } catch (final InterruptedException e) {
            break; // it's an intended InterruptedException to quit the thread
          }

          final EncodedKey<Integer> encodedKey = request.getLeft();
          final int requestId = request.getRight();
          workerHandler.processPullReply(encodedKey.getKey(), encodedKey.getKey(), requestId, 0);
        }
      }
    };

    // start threads that process pull requests from the keyToReplyQueue
    final ExecutorService executorService = Executors.newFixedThreadPool(NUM_PULL_HANDLING_THREADS);

    for (int i = 0; i < NUM_PULL_HANDLING_THREADS; i++) {
      executorService.execute(pullReplyingThread);
    }

    return executorService;
  }

  /**
   * Mocks {@link WorkerMsgSender#sendPullMsg(String, EncodedKey, int)}
   * to enqueue incoming operations into {@code pullKeyQueue}.
   */
  static void setupSenderToEnqueuePullOps(final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyQueue,
                                          final WorkerMsgSender<Integer, ?> workerMsgSender)
      throws NetworkException {
    // pull messages to asynchronous parameter worker should return values s.t. key == value
    doAnswer(invocationOnMock -> {
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];
        final int requestId = (Integer) invocationOnMock.getArguments()[2];
        // assume it always succeeds, otherwise it will throw IllegalArgumentException
        pullKeyQueue.add(Pair.of(encodedKey, requestId));
        return null;
      }).when(workerMsgSender).sendPullMsg(anyString(), anyObject(), anyInt());
  }

  static void close(final ParameterWorker<Integer, ?, Integer> worker,
                    final WorkerMsgSender<Integer, ?> workerMsgSender,
                    final WorkerHandler<Integer, ?, Integer> workerHandler)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();
    final ExecutorService executorService = startPullReplyingThreads(pullKeyToReplyQueue, workerHandler);
    setupSenderToEnqueuePullOps(pullKeyToReplyQueue, workerMsgSender);

    final ExecutorService pool = Executors.newSingleThreadExecutor();

    worker.close(CLOSE_TIMEOUT);

    pool.submit((Runnable) () -> {
        worker.pull(0);
        countDownLatch.countDown();
      });
    pool.shutdown();

    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    assertFalse(MSG_THREADS_SHOULD_NOT_FINISH, allThreadsFinished);

    executorService.shutdownNow();
  }

  static void multiThreadPush(final ParameterWorker<Integer, Integer, ?> worker,
                              final WorkerMsgSender<Integer, Integer> sender)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPushThreads = 8;
    final int numKeys = 4;
    final int numPushPerThread = 1000;
    final int pushValue = 1;

    final CountDownLatch countDownLatch = new CountDownLatch(numPushThreads);
    final Runnable[] threads = new Runnable[numPushThreads];

    for (int index = 0; index < numPushThreads; ++index) {
      final int key = index % numKeys;
      threads[index] = () -> {
        for (int push = 0; push < numPushPerThread; ++push) {
          worker.push(key, pushValue);
        }
        countDownLatch.countDown();
      };
    }

    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    worker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(sender, times(numPushThreads * numPushPerThread)).sendPushMsg(anyString(), anyObject(), eq(pushValue));
  }

  static void multiThreadPull(final ParameterWorker<Integer, ?, Integer> worker,
                              final WorkerMsgSender<Integer, ?> workerMsgSender,
                              final WorkerHandler<Integer, ?, Integer> workerHandler)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numKeys = 4;
    final int numPullPerThread = 1000;

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();
    final ExecutorService executorService = startPullReplyingThreads(pullKeyToReplyQueue, workerHandler);
    setupSenderToEnqueuePullOps(pullKeyToReplyQueue, workerMsgSender);

    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

    for (int index = 0; index < numPullThreads; ++index) {
      final int key = index % numKeys;
      threads[index] = () -> {
        for (int pull = 0; pull < numPullPerThread; ++pull) {
          final Integer val = worker.pull(key);
          if (val == null || !val.equals(key)) {
            correctResultReturned.set(false);
            break;
          }
        }
        countDownLatch.countDown();
      };
    }

    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);
    worker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());

    executorService.shutdownNow();
  }

  static void multiThreadMultiKeyPull(final ParameterWorker<Integer, ?, Integer> worker,
                                      final WorkerMsgSender<Integer, ?> workerMsgSender,
                                      final WorkerHandler<Integer, ?, Integer> workerHandler)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numKeys = 4;
    final int numPullPerThread = 1000;
    final int numKeysPerPull = 3;

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();
    final ExecutorService executorService = startPullReplyingThreads(pullKeyToReplyQueue, workerHandler);
    setupSenderToEnqueuePullOps(pullKeyToReplyQueue, workerMsgSender);

    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

    for (int index = 0; index < numPullThreads; ++index) {
      final int threadIndex = index;

      threads[index] = () -> {
        for (int pull = 0; pull < numPullPerThread; ++pull) {
          final List<Integer> keyList = new ArrayList<>(numKeysPerPull);
          for (int keyIndex = 0; keyIndex < numKeysPerPull; ++keyIndex) {
            keyList.add((threadIndex + pull + keyIndex) % numKeys);
          }

          final List<Integer> vals = worker.pull(keyList);
          for (int listIndex = 0; listIndex < keyList.size(); ++listIndex) {
            final Integer val = vals.get(listIndex);
            if (val == null || !val.equals(keyList.get(listIndex))) {
              correctResultReturned.set(false);
              break;
            }
          }
        }
        countDownLatch.countDown();
      };
    }

    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);
    worker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());

    executorService.shutdownNow();
  }

  /**
   * Starts threads that reject pull operations queued in {@code keyToRejectQueue}.
   * @return an {@link ExecutorService} through which the thread is executing.
   */
  private static ExecutorService startPullRejectingThreads(
      final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> keyToRejectQueue,
      final WorkerHandler<Integer, ?, Integer> handler,
      final int numRejectPerKey) {
    final Map<Integer, AtomicInteger> keyToNumPullCounter = new ConcurrentHashMap<>();

    final Runnable pullRejectingThread = new Runnable() {
      @Override
      public void run() {
        while (true) {
          final Pair<EncodedKey<Integer>, Integer> request;
          try {
            request = keyToRejectQueue.take();
          } catch (final InterruptedException e) {
            break; // it's an intended InterruptedException to quit the thread
          }

          final EncodedKey<Integer> encodedKey = request.getLeft();
          final int requestId = request.getRight();
          if (!keyToNumPullCounter.containsKey(encodedKey.getKey())) {
            keyToNumPullCounter.put(encodedKey.getKey(), new AtomicInteger(0));
          }

          final int numAnswerForTheKey = keyToNumPullCounter.get(encodedKey.getKey()).getAndIncrement();
          if (numAnswerForTheKey < numRejectPerKey) {
            handler.processPullReject(encodedKey.getKey(), requestId);
          } else {
            // pull messages should return values s.t. key == value
            handler.processPullReply(encodedKey.getKey(), encodedKey.getKey(), requestId, 0);
          }
        }
      }
    };

    // start threads that reject pull requests from the keyToRejectQueue
    final ExecutorService executorService = Executors.newFixedThreadPool(NUM_PULL_HANDLING_THREADS);

    for (int i = 0; i < NUM_PULL_HANDLING_THREADS; i++) {
      executorService.execute(pullRejectingThread);
    }

    return executorService;
  }

  static void pullReject(final ParameterWorker<Integer, ?, Integer> worker,
                         final WorkerHandler<Integer, ?, Integer> handler,
                         final WorkerMsgSender<Integer, ?> sender)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numPullPerThread = 1000;
    final int numRejectPerKey = AsyncParameterWorker.MAX_PULL_RETRY_COUNT / 2;

    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToRejectQueue = new LinkedBlockingQueue<>();
    final ExecutorService executorService = startPullRejectingThreads(pullKeyToRejectQueue, handler, numRejectPerKey);
    setupSenderToEnqueuePullOps(pullKeyToRejectQueue, sender);

    for (int index = 0; index < numPullThreads; ++index) {
      final int baseKey = index * numPullPerThread;
      threads[index] = () -> {
        for (int pull = 0; pull < numPullPerThread; pull++) {
          final int key = baseKey + pull;
          final Integer val = worker.pull(key);

          if (val == null || !val.equals(key)) {
            correctResultReturned.set(false);
            break;
          }
        }
        countDownLatch.countDown();
      };
    }

    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);
    worker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());
    verify(sender, times(numPullPerThread * numPullThreads * (numRejectPerKey + 1)))
        .sendPullMsg(anyString(), anyObject(), anyInt());

    executorService.shutdownNow();
  }

  static void pullNetworkExceptionAndResend(final ParameterWorker<Integer, ?, Integer> worker,
                                            final WorkerHandler<Integer, ?, Integer> handler,
                                            final WorkerMsgSender<Integer, ?> sender)
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException {
    final AtomicInteger sendPullCounter = new AtomicInteger(0);
    final CountDownLatch finishLatch = new CountDownLatch(1);
    final long gracePeriodMs = 100; // a time period to make sure all resend requests have been sent.
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    // throw exception when worker sends a pull msg
    doAnswer(invocationOnMock -> {
        if (sendPullCounter.getAndIncrement() < AsyncParameterWorker.MAX_RESEND_COUNT) {
          throw new NetworkException("exception");
        }
        finishLatch.countDown();

        // reply at the last chance to prevent PS worker thread from throwing RuntimeException
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];
        final int requestId = (int) invocationOnMock.getArguments()[2];
        handler.processPullReply(encodedKey.getKey(), encodedKey.getKey(), requestId, 0);
        return null;
      }).when(sender).sendPullMsg(anyString(), any(EncodedKey.class), anyInt());

    final int key = 0;

    pool.execute(() -> worker.pull(key));

    worker.close(CLOSE_TIMEOUT);
    pool.shutdown();

    assertTrue(finishLatch.await((AsyncParameterWorker.RESEND_INTERVAL_MS + gracePeriodMs)
        * AsyncParameterWorker.MAX_RESEND_COUNT, TimeUnit.MILLISECONDS));

    // Check whether the expected number of pull requests have been made
    // (1 initial attempt + MAX_RESEND_COUNT retry).
    verify(sender, times(AsyncParameterWorker.MAX_RESEND_COUNT + 1))
        .sendPullMsg(anyString(), any(EncodedKey.class), anyInt());
  }

  static void pushNetworkExceptionAndResend(final ParameterWorker<Integer, Integer, ?> worker,
                                            final WorkerMsgSender<Integer, ?> sender)
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException {
    final AtomicInteger sendPushCounter = new AtomicInteger(0);
    final CountDownLatch finishLatch = new CountDownLatch(1);
    final long gracePeriodMs = 100; // a time period to make sure all resend requests have been sent.
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    // throw exception when worker sends a push msg
    doAnswer(invocationOnMock -> {
        // skip at the last chance to prevent PS worker thread from throwing RuntimeException
        if (sendPushCounter.getAndIncrement() < AsyncParameterWorker.MAX_RESEND_COUNT) {
          throw new NetworkException("exception");
        }
        finishLatch.countDown();
        return null;
      }).when(sender).sendPushMsg(anyString(), any(EncodedKey.class), anyObject());

    final int key = 0;

    pool.execute(() -> worker.push(key, key));

    worker.close(CLOSE_TIMEOUT);
    pool.shutdown();

    assertTrue(finishLatch.await((AsyncParameterWorker.RESEND_INTERVAL_MS + gracePeriodMs)
        * AsyncParameterWorker.MAX_RESEND_COUNT, TimeUnit.MILLISECONDS));

    // Check whether the expected number of push requests have been made
    // (1 initial attempt + MAX_RESEND_COUNT retry).
    verify(sender, times(AsyncParameterWorker.MAX_RESEND_COUNT + 1)).sendPushMsg(anyString(), any(EncodedKey.class),
        anyObject());
  }

  static void pullTimeoutAndRetry(final ParameterWorker<Integer, ?, Integer> worker,
                                  final WorkerHandler<Integer, ?, Integer> handler,
                                  final WorkerMsgSender<Integer, ?> sender)
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException {
    final AtomicInteger sendPullCounter = new AtomicInteger(0);
    final CountDownLatch finishLatch = new CountDownLatch(1);
    final long gracePeriodMs = 100; // a time period to make sure all retry requests have been sent.
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    final int key = 0;

    // Do not reply when worker sends a pull msg
    doAnswer(invocationOnMock -> {
        if (sendPullCounter.getAndIncrement() < AsyncParameterWorker.MAX_PULL_RETRY_COUNT) {
          return null;
        }
        finishLatch.countDown();

        // reply at the last chance to prevent PS worker thread from throwing RuntimeException
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];
        final int requestId = (int) invocationOnMock.getArguments()[2];
        handler.processPullReply(encodedKey.getKey(), encodedKey.getKey(), requestId, 0);

        return null;
      }).when(sender).sendPullMsg(anyString(), anyObject(), anyInt());

    pool.execute(() -> worker.pull(key));

    // need to wait for PULL_RETRY_TIMEOUT_MS * 2 for one retry, because of the implementation of parameter worker
    assertTrue(finishLatch.await(PULL_RETRY_TIMEOUT_MS * 2 + (PULL_RETRY_TIMEOUT_MS * 2 + gracePeriodMs)
        * AsyncParameterWorker.MAX_PULL_RETRY_COUNT, TimeUnit.MILLISECONDS));
    worker.close(CLOSE_TIMEOUT);
    pool.shutdown();

    // Check whether the expected number of pull requests have been made
    // (1 initial attempt + MAX_PULL_RETRY_COUNT retry).
    verify(sender, times(AsyncParameterWorker.MAX_PULL_RETRY_COUNT + 1))
        .sendPullMsg(anyString(), any(EncodedKey.class), anyInt());
  }
}
