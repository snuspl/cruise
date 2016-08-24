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
import org.apache.reef.exception.evaluator.NetworkException;

import java.util.ArrayList;
import java.util.HashMap;
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
public class ParameterWorkerTestUtil {
  public static final long CLOSE_TIMEOUT = 5000;
  public static final long PULL_RETRY_TIMEOUT_MS = 1000;
  public static final String MSG_THREADS_SHOULD_FINISH = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_THREADS_SHOULD_NOT_FINISH = "threads have finished but should not";
  private static final String MSG_RESULT_ASSERTION = "threads received incorrect values";

  public void close(final ParameterWorker worker)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    worker.close(CLOSE_TIMEOUT);

    pool.submit((Runnable) () -> {
        worker.pull(0);
        countDownLatch.countDown();
      });
    pool.shutdown();

    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    assertFalse(MSG_THREADS_SHOULD_NOT_FINISH, allThreadsFinished);
  }

  public void multiThreadPush(final ParameterWorker worker, final WorkerMsgSender sender)
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

  public void multiThreadPull(final ParameterWorker<Integer, Integer, Integer> worker)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numKeys = 4;
    final int numPullPerThread = 1000;
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
  }

  public void multiThreadMultiKeyPull(final ParameterWorker worker)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numKeys = 4;
    final int numPullPerThread = 1000;
    final int numKeysPerPull = 3;
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
  }


  public void pullReject(final ParameterWorker<Integer, Integer, Integer> worker,
                         final WorkerHandler handler, final WorkerMsgSender sender)
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numPullPerThread = 1000;
    final int numRejectPerKey = AsyncParameterWorker.MAX_PULL_RETRY_COUNT / 2;

    final Map<Integer, AtomicInteger> keyToNumPullCounter = new HashMap<>();
    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

    final BlockingQueue<EncodedKey<Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();

    // start a thread that process pull requests from the pullKeyToReplyQueue
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            // send reject msg immediately to prevent worker from retrying pull operation
            final EncodedKey<Integer> encodedKey = pullKeyToReplyQueue.take();

            if (!keyToNumPullCounter.containsKey(encodedKey.getKey())) {
              keyToNumPullCounter.put(encodedKey.getKey(), new AtomicInteger(0));
            }

            final int numAnswerForTheKey = keyToNumPullCounter.get(encodedKey.getKey()).getAndIncrement();

            if (numAnswerForTheKey < numRejectPerKey) {
              handler.processPullReject(encodedKey.getKey());
            } else {
              // pull messages should return values s.t. key == value
              handler.processPullReply(encodedKey.getKey(), encodedKey.getKey(), 0);
            }

          } catch (final InterruptedException e) {
            break; // it's an intended InterruptedException to quit the thread
          }
        }
      }
    });

    // put key of pull msgs into pullKeyToReplyQueue
    doAnswer(invocationOnMock -> {
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];

        pullKeyToReplyQueue.put(encodedKey);

        return null;
      }).when(sender).sendPullMsg(anyString(), anyObject());

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

    executorService.shutdownNow(); // it will interrupt all running threads

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());
    verify(sender, times(numPullPerThread * numPullThreads * (numRejectPerKey + 1)))
        .sendPullMsg(anyString(), anyObject());
  }



  public void pullNetworkExceptionAndResend(final ParameterWorker worker, final WorkerMsgSender sender)
      throws NetworkException, InterruptedException {
    final CountDownLatch sendLatch = new CountDownLatch(1 + AsyncParameterWorker.MAX_RESEND_COUNT);
    final long gracePeriodMs = 100; // a time period to make sure all resend requests have been sent.
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    // throw exception when worker sends a pull msg
    doAnswer(invocationOnMock -> {
        sendLatch.countDown();
        throw new NetworkException("exception");
      }).when(sender).sendPullMsg(anyString(), any(EncodedKey.class));

    final int key = 0;

    pool.execute(() -> worker.pull(key));
    pool.shutdown();

    assertTrue(sendLatch.await((AsyncParameterWorker.RESEND_INTERVAL_MS + gracePeriodMs)
        * AsyncParameterWorker.MAX_RESEND_COUNT, TimeUnit.MILLISECONDS));

    // Check whether the expected number of pull requests have been made
    // (1 initial attempt + MAX_RESEND_COUNT resend).
    verify(sender, times(1 + AsyncParameterWorker.MAX_RESEND_COUNT)).sendPullMsg(anyString(), any(EncodedKey.class));
  }




  public void pushNetworkExceptionAndResend(final ParameterWorker worker, final WorkerMsgSender sender)
      throws NetworkException, InterruptedException {
    final CountDownLatch sendLatch = new CountDownLatch(1 + AsyncParameterWorker.MAX_RESEND_COUNT);
    final long gracePeriodMs = 100; // a time period to make sure all resend requests have been sent.
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    // throw exception when worker sends a push msg
    doAnswer(invocationOnMock -> {
        sendLatch.countDown();
        throw new NetworkException("exception");
      }).when(sender).sendPushMsg(anyString(), any(EncodedKey.class), anyObject());

    final int key = 0;

    pool.execute(() -> worker.push(key, key));
    pool.shutdown();

    assertTrue(sendLatch.await((AsyncParameterWorker.RESEND_INTERVAL_MS + gracePeriodMs)
        * AsyncParameterWorker.MAX_RESEND_COUNT, TimeUnit.MILLISECONDS));

    // Check whether the expected number of push requests have been made
    // (1 initial attempt + MAX_RESEND_COUNT resend).
    verify(sender, times(1 + AsyncParameterWorker.MAX_RESEND_COUNT)).sendPushMsg(anyString(), any(EncodedKey.class),
        anyObject());
  }


  public void pullTimeoutAndRetry(final ParameterWorker worker, final WorkerMsgSender sender)
      throws NetworkException, InterruptedException {
    final CountDownLatch sendLatch = new CountDownLatch(1 + AsyncParameterWorker.MAX_PULL_RETRY_COUNT);
    final long gracePeriodMs = 100; // a time period to make sure all retry requests have been sent.
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    final int key = 0;

    // Do noy reply when worker sends a pull msg
    doAnswer(invocationOnMock -> {
        sendLatch.countDown();
        return null;
      }).when(sender).sendPullMsg(anyString(), anyObject());

    pool.execute(() -> worker.pull(key));
    pool.shutdown();

    assertTrue(sendLatch.await((PULL_RETRY_TIMEOUT_MS + gracePeriodMs) * AsyncParameterWorker.MAX_PULL_RETRY_COUNT,
        TimeUnit.MILLISECONDS));

    // Check whether the expected number of pull requests have been made
    // (1 initial attempt + MAX_PULL_RETRY_COUNT retry).
    verify(sender, times(1 + AsyncParameterWorker.MAX_PULL_RETRY_COUNT)).sendPullMsg(anyString(),
        any(EncodedKey.class));
  }
}
