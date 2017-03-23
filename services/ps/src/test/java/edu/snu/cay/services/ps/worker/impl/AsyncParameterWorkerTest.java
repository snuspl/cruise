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

import edu.snu.cay.services.ps.PSParameters;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.parameters.ParameterWorkerNumThreads;
import edu.snu.cay.services.ps.worker.parameters.PullRetryTimeoutMs;
import edu.snu.cay.services.ps.worker.parameters.WorkerQueueSize;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import edu.snu.cay.utils.EnforceLoggingLevelRule;
import edu.snu.cay.utils.ThreadUtils;
import edu.snu.cay.utils.test.IntensiveTests;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.htrace.SpanReceiver;
import org.htrace.TraceInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static edu.snu.cay.services.ps.worker.impl.ParameterWorkerTestUtil.*;
import static edu.snu.cay.services.ps.worker.parameters.PullRetryTimeoutMs.TIMEOUT_NO_RETRY;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AsyncParameterWorker}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerMsgSender.class)
public final class AsyncParameterWorkerTest {
  private static final int WORKER_QUEUE_SIZE = 2500;

  // Those metrics are not used in the tests, so can be set 0,
  private static final long SERVER_PROCESSING_TIME = 0;
  private static final int NUM_RECEIVED_BYTES = 0;
  private static final int WORKER_NUM_THREADS = 2;
  private static final TraceInfo EMPTY_TRACE = null;

  private ParameterWorker<Integer, Integer, Integer> parameterWorker;
  private WorkerHandler<Integer, Integer, Integer> workerHandler;
  private WorkerMsgSender<Integer, Integer> mockSender;

  /**
   * Prepares PS components.
   * It mocks several message senders and handlers for testing.
   *
   * @param retryTimeoutMs a timeout to retry sending request, which is bound to {@link PullRetryTimeoutMs}
   * @throws InjectionException
   * @throws NetworkException
   */
  private void prepare(final long retryTimeoutMs) throws InjectionException, NetworkException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(WorkerQueueSize.class, Integer.toString(WORKER_QUEUE_SIZE))
        .bindNamedParameter(ParameterWorkerNumThreads.class, Integer.toString(WORKER_NUM_THREADS))
        .bindNamedParameter(PullRetryTimeoutMs.class, Long.toString(retryTimeoutMs))
        .bindNamedParameter(PSParameters.KeyCodecName.class, SerializableCodec.class)
        .bindImplementation(WorkerHandler.class, AsyncParameterWorker.class)
        .bindImplementation(ParameterWorker.class, AsyncParameterWorker.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    mockSender = mock(WorkerMsgSender.class);

    injector.bindVolatileInstance(WorkerMsgSender.class, mockSender);
    injector.bindVolatileInstance(ParameterUpdater.class, mock(ParameterUpdater.class));
    injector.bindVolatileInstance(ServerResolver.class, mock(ServerResolver.class));
    injector.bindVolatileInstance(SpanReceiver.class, mock(SpanReceiver.class));
    parameterWorker = injector.getInstance(ParameterWorker.class);
    workerHandler = injector.getInstance(WorkerHandler.class);
  }

  /**
   * Test that {@link AsyncParameterWorker#close(long)} does indeed block further operations from being processed.
   */
  @Test
  @Category(IntensiveTests.class)
  public void testClose()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.close(parameterWorker, mockSender, workerHandler);
  }

  /**
   * Test the thread safety of {@link AsyncParameterWorker} by
   * creating multiple threads that try to push values to the server using {@link AsyncParameterWorker}.
   *
   * {@code numPushThreads} threads are generated, each sending {@code numPushPerThread} pushes.
   */
  @Test
  public void testMultiThreadPush()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.multiThreadPush(parameterWorker, mockSender);
  }

  /**
   * Test the thread safety of {@link AsyncParameterWorker}
   * by creating multiple threads that try to pull values from the server using {@link AsyncParameterWorker}.
   *
   * {@code numPullThreads} threads are generated, each sending {@code numPullPerThread} pulls.
   * Due to the cache, {@code sender.sendPullMsg()} may not be invoked as many times as {@code worker.pull()} is called.
   * Thus, we verify the validity of the result by simply checking whether pulled values are as expected or not.
   */
  @Test
  public void testMultiThreadPull()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.multiThreadPull(parameterWorker, mockSender, workerHandler);
  }

  /**
   * Test the thread safety of {@link AsyncParameterWorker} by
   * creating multiple threads that try to pull several values from the server using {@link AsyncParameterWorker}.
   *
   * {@code numPullThreads} threads are generated, each sending {@code numPullPerThread} pulls.
   * For each pull, {@code numKeysPerPull} keys are selected, based on the thread index and the pull count.
   */
  @Test
  public void testMultiThreadMultiKeyPull()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.multiThreadMultiKeyPull(parameterWorker, mockSender, workerHandler);
  }

  /**
   * Rule for suppressing massive WARNING level logs by {@link NetworkException}
   * while {@link AsyncParameterWorker} tries to send a pull msg,
   * which are intentionally caused many times in {@link #testPullNetworkExceptionAndResend()}.
   */
  @Rule
  private TestRule pullResendWatcher = new EnforceLoggingLevelRule("testPullNetworkExceptionAndResend",
      AsyncParameterWorker.class.getName(), Level.SEVERE);

  /**
   * Tests whether worker correctly resends the pull operation, when network exception happens.
   */
  @Test
  public void testPullNetworkExceptionAndResend()
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.pullNetworkExceptionAndResend(parameterWorker, workerHandler, mockSender);
  }

  /**
   * Rule for suppressing massive WARNING level logs by {@link NetworkException}
   * while {@link AsyncParameterWorker} tries to send a push msg,
   * which are intentionally caused many times in {@link #testPushNetworkExceptionAndResend()}.
   */
  @Rule
  private TestRule pushResendWatcher = new EnforceLoggingLevelRule("testPushNetworkExceptionAndResend",
      AsyncParameterWorker.class.getName(), Level.SEVERE);

  /**
   * Tests whether worker correctly resends the push operation, when network exception happens.
   */
  @Test
  public void testPushNetworkExceptionAndResend()
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.pushNetworkExceptionAndResend(parameterWorker, mockSender);
  }

  /**
   * Tests whether worker correctly restart the pull operation, when the server does not respond within timeout.
   */
  @Test
  @Category(IntensiveTests.class)
  public void testPullTimeoutAndRetry()
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException, InjectionException {
    prepare(ParameterWorkerTestUtil.PULL_RETRY_TIMEOUT_MS);
    ParameterWorkerTestUtil.pullTimeoutAndRetry(parameterWorker, workerHandler, mockSender);
  }

  /**
   * Test that the {@link AsyncParameterWorker#invalidateAll()} method invalidate all caches
   * so that new pull messages must be issued for each pull request.
   */
  @Test
  public void testInvalidateAll()
      throws InterruptedException, ExecutionException, TimeoutException, NetworkException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();
    final ExecutorService executorService =
        ParameterWorkerTestUtil.startPullReplyingThreads(pullKeyToReplyQueue, workerHandler);
    ParameterWorkerTestUtil.setupSenderToEnqueuePullOps(pullKeyToReplyQueue, mockSender);

    final int numPulls = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    // invalidateAll() is not exposed to interface level
    // so we need to cast to implementation level
    final AsyncParameterWorker<Integer, Integer, Integer> asyncParameterWorker =
        (AsyncParameterWorker<Integer, Integer, Integer>) parameterWorker;

    pool.submit(() -> {
      for (int pull = 0; pull < numPulls; ++pull) {
        asyncParameterWorker.pull(0);
        asyncParameterWorker.invalidateAll();
      }
      countDownLatch.countDown();
    });
    pool.shutdown();

    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    asyncParameterWorker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(mockSender, times(numPulls)).sendPullMsg(anyString(), anyObject(), anyInt(), any(TraceInfo.class));

    executorService.shutdown();
  }

  /**
   * Test whether parameter worker waits for corresponding pull reply before processing push operation.
   */
  @Test
  public void testPushAfterPull()
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();
    ParameterWorkerTestUtil.setupSenderToEnqueuePullOps(pullKeyToReplyQueue, mockSender);
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    final int key = 0;
    final int pushValue = 1;
    final long gracePeriodMs = 100;
    final long waitingMs = 3000;

    final CountDownLatch countDownLatch = new CountDownLatch(1);

    pool.execute(() -> {
      parameterWorker.pull(key);
      countDownLatch.countDown();
    });
    pool.shutdown();
    Thread.sleep(gracePeriodMs);
    parameterWorker.push(key, pushValue);
    Thread.sleep(waitingMs);

    // assert that mockSender sent 1 pull message and 0 push message
    // in other words, push is waiting for pull to be replied
    verify(mockSender, times(1)).sendPullMsg(anyString(), anyObject(), anyInt(), any(TraceInfo.class));
    verify(mockSender, times(0)).sendPushMsg(anyString(), anyObject(), anyInt());
    assertEquals(MSG_THREADS_SHOULD_NOT_FINISH, 1, countDownLatch.getCount()); // have not received pull reply yet

    final Pair<EncodedKey<Integer>, Integer> request = pullKeyToReplyQueue.take();
    final EncodedKey<Integer> encodedKey = request.getLeft();
    final int requestId = request.getRight();
    workerHandler.processPullReply(encodedKey.getKey(), encodedKey.getKey(), requestId,
        SERVER_PROCESSING_TIME, NUM_RECEIVED_BYTES, EMPTY_TRACE);
    Thread.sleep(gracePeriodMs);

    final boolean allThreadsFinished = countDownLatch.await(waitingMs, TimeUnit.SECONDS);
    parameterWorker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(mockSender, times(1)).sendPushMsg(anyString(), anyObject(), anyInt());
  }

  /**
   * Test whether parameter worker handles multiple pull operations with one request to server if available.
   */
  @Test
  public void testMultiplePull()
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();
    ParameterWorkerTestUtil.setupSenderToEnqueuePullOps(pullKeyToReplyQueue, mockSender);

    final int numPullThreads = 8;
    final int key = 0;
    final long waitingMs = 1000;

    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

    for (int index = 0; index < numPullThreads; ++index) {
      threads[index] = () -> {
        final Integer val = parameterWorker.pull(key);
        if (val == null || !val.equals(key)) {
          correctResultReturned.set(false);
        }
        countDownLatch.countDown();
      };
    }

    ThreadUtils.runConcurrently(threads);
    // wait for all pull operations are processed
    Thread.sleep(waitingMs);

    // have not received pull reply yet
    assertEquals(MSG_THREADS_SHOULD_NOT_FINISH, 8, countDownLatch.getCount());

    final Pair<EncodedKey<Integer>, Integer> request = pullKeyToReplyQueue.take();
    final EncodedKey<Integer> encodedKey = request.getLeft();
    final int requestId = request.getRight();
    workerHandler.processPullReply(encodedKey.getKey(), encodedKey.getKey(), requestId,
        SERVER_PROCESSING_TIME, NUM_RECEIVED_BYTES, EMPTY_TRACE);

    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);
    parameterWorker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());
    // should send pull request only once
    verify(mockSender, times(1)).sendPullMsg(anyString(), anyObject(), anyInt(), any(TraceInfo.class));
  }
}
