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
import edu.snu.cay.services.ps.common.resolver.ServerId;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.parameters.ParameterWorkerNumThreads;
import edu.snu.cay.services.ps.worker.parameters.PullRetryTimeoutMs;
import edu.snu.cay.services.ps.worker.parameters.WorkerQueueSize;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
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

  private ParameterWorkerTestUtil testUtil;
  private AsyncParameterWorker<Integer, Integer, Integer> parameterWorker;
  private WorkerHandler<Integer, Integer, Integer> workerHandler;
  private WorkerMsgSender<Integer, Integer> mockSender;

  @Before
  public void setup() throws InjectionException, NetworkException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ServerId.class, "ServerId")
        .bindNamedParameter(WorkerQueueSize.class, Integer.toString(WORKER_QUEUE_SIZE))
        .bindNamedParameter(ParameterWorkerNumThreads.class, Integer.toString(WORKER_NUM_THREADS))
        .bindNamedParameter(PullRetryTimeoutMs.class, Long.toString(ParameterWorkerTestUtil.PULL_RETRY_TIMEOUT_MS))
        .bindNamedParameter(PSParameters.KeyCodecName.class, SerializableCodec.class)
        .bindImplementation(WorkerHandler.class, AsyncParameterWorker.class)
        .bindImplementation(ParameterWorker.class, AsyncParameterWorker.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    testUtil = new ParameterWorkerTestUtil();

    mockSender = mock(WorkerMsgSender.class);

    injector.bindVolatileInstance(WorkerMsgSender.class, mockSender);
    injector.bindVolatileInstance(ParameterUpdater.class, mock(ParameterUpdater.class));
    injector.bindVolatileInstance(ServerResolver.class, mock(ServerResolver.class));

    // pull messages to asynchronous parameter worker should return values s.t. key == value
    doAnswer(invocationOnMock -> {
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];
        workerHandler.processPullReply(encodedKey.getKey(), encodedKey.getKey());
        return null;
      }).when(mockSender).sendPullMsg(anyString(), anyObject());

    parameterWorker = (AsyncParameterWorker) injector.getInstance(ParameterWorker.class);
    workerHandler = injector.getInstance(WorkerHandler.class);
  }

  /**
   * Test that {@link AsyncParameterWorker#close(long)} does indeed block further operations from being processed.
   */
  @Test
  public void testClose() throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    testUtil.close(parameterWorker);
  }

  /**
   * Test the thread safety of {@link AsyncParameterWorker} by
   * creating multiple threads that try to push values to the server using {@link AsyncParameterWorker}.
   *
   * {@code numPushThreads} threads are generated, each sending {@code numPushPerThread} pushes.
   */
  @Test
  public void testMultiThreadPush()
          throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    testUtil.multiThreadPush(parameterWorker, mockSender);
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
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    testUtil.multiThreadPull(parameterWorker);
  }

  /**
   * Test the thread safety of {@link AsyncParameterWorker} by
   * creating multiple threads that try to pull several values from the server using {@link AsyncParameterWorker}.
   *
   * {@code numPullThreads} threads are generated, each sending {@code numPullPerThread} pulls.
   * For each pull, {@code numKeysPerPull} keys are selected, based on the thread index and the pull count.
   */
  @Test
  public void testMultiThreadMultiKeyPull() throws InterruptedException, TimeoutException,
          ExecutionException, NetworkException {
    testUtil.multiThreadMultiKeyPull(parameterWorker);
  }

  /**
   * Rule for suppressing massive logging of INFO level in msg reject.
   */
  @Rule
  public TestRule watcher = new TestWatcherLoggingRule("testPullReject",
      AsyncParameterWorker.class.getName(), Level.WARNING);

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
          throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    testUtil.pullReject(parameterWorker, workerHandler, mockSender);
  }

  /**
   * Tests whether worker correctly resend the pull operation, when network exception happens.
   */
  @Test
  public void testPullNetworkExceptionAndResend() throws NetworkException, InterruptedException {
    testUtil.pullNetworkExceptionAndResend(parameterWorker, mockSender);
  }

  /**
   * Tests whether worker correctly resend the push operation, when network exception happens.
   */
  @Test
  public void testPushNetworkExceptionAndResend() throws NetworkException, InterruptedException {
    testUtil.pushNetworkExceptionAndResend(parameterWorker, mockSender);
  }

  /**
   * Tests whether worker correctly restart the pull operation, when the server does not respond within timeout.
   */
  @Test
  public void testPullTimeoutAndRetry() throws NetworkException, InterruptedException {
    testUtil.pullTimeoutAndRetry(parameterWorker, mockSender);
  }

  /**
   * Test that the {@link AsyncParameterWorker#invalidateAll()} method invalidate all caches
   * so that new pull messages must be issued for each pull request.
   */
  @Test
  public void testInvalidateAll()
          throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    invalidateAllAsync(parameterWorker);
  }

  private void invalidateAllAsync(final AsyncParameterWorker worker)
          throws InterruptedException, ExecutionException, TimeoutException, NetworkException {
    final int numPulls = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    pool.submit(() -> {
        for (int pull = 0; pull < numPulls; ++pull) {
          worker.pull(0);
          worker.invalidateAll();
        }
        countDownLatch.countDown();
      });
    pool.shutdown();

    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    worker.close(ParameterWorkerTestUtil.CLOSE_TIMEOUT);

    assertTrue(ParameterWorkerTestUtil.MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(mockSender, times(numPulls)).sendPullMsg(anyString(), anyObject());
  }
}
