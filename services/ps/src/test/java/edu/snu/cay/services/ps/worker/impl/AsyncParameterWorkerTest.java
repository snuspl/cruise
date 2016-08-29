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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.*;
import java.util.logging.Level;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AsyncParameterWorker}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerMsgSender.class)
public final class AsyncParameterWorkerTest {
  private static final int WORKER_QUEUE_SIZE = 2500;
  private static final int WORKER_NUM_THREADS = 2;

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
    parameterWorker = injector.getInstance(ParameterWorker.class);
    workerHandler = injector.getInstance(WorkerHandler.class);
  }

  /**
   * Test that {@link AsyncParameterWorker#close(long)} does indeed block further operations from being processed.
   */
  @Test
  public void testClose()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(0);
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
    prepare(0);
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
    prepare(0);
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
    prepare(0);
    ParameterWorkerTestUtil.multiThreadMultiKeyPull(parameterWorker, mockSender, workerHandler);
  }

  /**
   * Rule for suppressing massive WARNING level logs in {@link AsyncParameterWorker#processPullReject},
   * which are intentionally called many times in {@link #testPullReject}.
   */
  @Rule
  private TestRule pullRejectWatcher = new EnforceLoggingLevelRule("testPullReject",
      AsyncParameterWorker.class.getName(), Level.SEVERE);

  /**
   * Test the correct handling of pull rejects by {@link AsyncParameterWorker},
   * creating multiple threads that try to pull values from the server using {@link AsyncParameterWorker}.
   *
   * {@code numPullThreads} threads are generated, each sending {@code numPullPerThread} pulls.
   * To guarantee that {@code sender.sendPullMsg()} should be invoked as many times as {@code worker.pull()} is called,
   * this test use different keys for each pull.
   */
  @Test
  public void testPullReject()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(0);
    ParameterWorkerTestUtil.pullReject(parameterWorker, workerHandler, mockSender);
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
    prepare(0);
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
    prepare(0);
    ParameterWorkerTestUtil.pushNetworkExceptionAndResend(parameterWorker, mockSender);
  }

  /**
   * Tests whether worker correctly restart the pull operation, when the server does not respond within timeout.
   */
  @Test
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
    prepare(0);

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
    asyncParameterWorker.close(ParameterWorkerTestUtil.CLOSE_TIMEOUT);

    assertTrue(ParameterWorkerTestUtil.MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(mockSender, times(numPulls)).sendPullMsg(anyString(), anyObject(), anyInt());

    executorService.shutdown();
  }
}
