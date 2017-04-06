/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.cay.common.centcomm.master.MasterSideCentCommMsgSender;
import edu.snu.cay.common.centcomm.slave.SlaveSideCentCommMsgSender;
import edu.snu.cay.services.ps.PSParameters;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.api.WorkerClock;
import edu.snu.cay.services.ps.worker.parameters.ParameterWorkerNumThreads;
import edu.snu.cay.services.ps.worker.parameters.PullRetryTimeoutMs;
import edu.snu.cay.services.ps.worker.parameters.WorkerQueueSize;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import edu.snu.cay.utils.EnforceLoggingLevelRule;
import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.services.ps.avro.AvroClockMsg;
import edu.snu.cay.services.ps.driver.impl.ClockManager;
import edu.snu.cay.services.ps.ns.ClockMsgCodec;
import edu.snu.cay.services.ps.worker.parameters.StalenessBound;
import edu.snu.cay.utils.ThreadUtils;
import edu.snu.cay.utils.test.IntensiveTest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.IdentifierFactory;
import org.htrace.TraceInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.logging.Level;

import static edu.snu.cay.services.ps.worker.parameters.PullRetryTimeoutMs.TIMEOUT_NO_RETRY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link SSPParameterWorker}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerMsgSender.class, SlaveSideCentCommMsgSender.class, MasterSideCentCommMsgSender.class})
public final class SSPParameterWorkerTest {
  private static final int WORKER_QUEUE_SIZE = 2500;
  private static final int WORKER_NUM_THREADS = 2;
  private static final int INIT_GLOBAL_MIN_CLOCK = 10;
  private static final int INIT_WORKER_CLOCK = INIT_GLOBAL_MIN_CLOCK;
  private static final int STALENESS_BOUND = 5;

  // the value of both IDs are meaningless
  private static final String DRIVER_ID = "driver";
  private static final String WORKER_ID = "worker";

  private ParameterWorker<Integer, Integer, Integer> parameterWorker;
  private WorkerHandler<Integer, Integer, Integer> workerHandler;
  private WorkerMsgSender<Integer, Integer> mockSender;
  private MasterSideCentCommMsgSender mockMasterSideCentCommMsgSender;
  private ClockMsgCodec codec;
  private SSPWorkerClock.MessageHandler sspWorkerClockMessageHandler;
  private WorkerClock workerClock;

  /**
   * Prepares PS components, including SSP components.
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
        .bindNamedParameter(StalenessBound.class, Integer.toString(STALENESS_BOUND))
        .bindImplementation(ParameterWorker.class, SSPParameterWorker.class)
        .bindImplementation(WorkerHandler.class, SSPParameterWorker.class)
        .bindImplementation(WorkerClock.class, SSPWorkerClock.class)
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    this.mockSender = mock(WorkerMsgSender.class);
    injector.bindVolatileInstance(WorkerMsgSender.class, this.mockSender);
    injector.bindVolatileInstance(ParameterUpdater.class, mock(ParameterUpdater.class));
    injector.bindVolatileInstance(ServerResolver.class, mock(ServerResolver.class));

    this.mockMasterSideCentCommMsgSender = mock(MasterSideCentCommMsgSender.class);
    injector.bindVolatileInstance(MasterSideCentCommMsgSender.class, this.mockMasterSideCentCommMsgSender);
    injector.bindVolatileInstance(SlaveSideCentCommMsgSender.class, mock(SlaveSideCentCommMsgSender.class));

    this.workerClock = injector.getInstance(WorkerClock.class);
    this.sspWorkerClockMessageHandler = injector.getInstance(SSPWorkerClock.MessageHandler.class);
    this.codec = injector.getInstance(ClockMsgCodec.class);

    this.parameterWorker = injector.getInstance(ParameterWorker.class);
    this.workerHandler = injector.getInstance(WorkerHandler.class);

    initWorkerClock();
    setupClockUpdateHandler();
  }

  /**
   * Initialize worker clock.
   * Required only by {@link #testDataStalenessCheck()} and {@link #testWorkerStalenessCheck()}.
   */
  private void initWorkerClock() {
    final AvroClockMsg initClockMsg =
        ClockManager.getReplyInitialClockMessage(INIT_GLOBAL_MIN_CLOCK, INIT_WORKER_CLOCK);
    final byte[] replyData = codec.encode(initClockMsg);
    final CentCommMsg sentCommMsg = getTestCentCommMsg(DRIVER_ID, replyData);
    sspWorkerClockMessageHandler.onNext(sentCommMsg);
  }

  /**
   * Mocks {@link MasterSideCentCommMsgSender#send(String, String, byte[])}
   * to update worker clock through {@link SSPWorkerClock.MessageHandler}
   * Required only by {@link #testDataStalenessCheck()} and {@link #testWorkerStalenessCheck()}.
   */
  private void setupClockUpdateHandler() throws NetworkException {
    // Stub to simulate the behavior of SSPWorkerClock.MessageHandler.
    // The message handler responds to the Aggregation message from Driver, which consists of the global minimum clock.
    doAnswer(invocation -> {
      final byte[] initClockMsgData = invocation.getArgumentAt(2, byte[].class);
      final CentCommMsg centCommMsg = getTestCentCommMsg(DRIVER_ID, initClockMsgData);
      sspWorkerClockMessageHandler.onNext(centCommMsg);
      return null;
    }).when(mockMasterSideCentCommMsgSender).send(anyString(), anyString(), anyObject());
  }

  /**
   * Test that {@link SSPParameterWorker#close(long)} does indeed block further operations from being processed.
   */
  @Test
  @Category(IntensiveTest.class)
  public void testClose()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.close(parameterWorker, mockSender, workerHandler);
  }

  /**
   * Test the thread safety of {@link SSPParameterWorker} by
   * creating multiple threads that try to push values to the server using {@link SSPParameterWorker}.
   * {@code numPushThreads} threads are generated, each sending {@code numPushPerThread} pushes.
   */
  @Test
  public void testMultiThreadPush()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.multiThreadPush(parameterWorker, mockSender);
  }

  /**
   * Test the thread safety of {@link SSPParameterWorker} by
   * creating multiple threads that try to pull values from the server
   * using {@link SSPParameterWorker}.
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
   * Test the thread safety of {@link SSPParameterWorker} by
   * creating multiple threads that try to pull several values from the server using {@link SSPParameterWorker}.
   */
  @Test
  public void testMultiThreadMultiKeyPull()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.multiThreadMultiKeyPull(parameterWorker, mockSender, workerHandler);
  }

  /**
   * Rule for suppressing massive WARNING level logs by {@link NetworkException}
   * while {@link SSPParameterWorker} tries to send a pull msg,
   * which are intentionally caused many times in {@link #testPullNetworkExceptionAndResend()}.
   */
  @Rule
  private TestRule pullResendWatcher = new EnforceLoggingLevelRule("testPullNetworkExceptionAndResend",
      SSPParameterWorker.class.getName(), Level.SEVERE);

  /**
   * Tests whether worker correctly resend the pull operation, when network exception happens.
   */
  @Test
  public void testPullNetworkExceptionAndResend()
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);
    ParameterWorkerTestUtil.pullNetworkExceptionAndResend(parameterWorker, workerHandler, mockSender);
  }

  /**
   * Rule for suppressing massive WARNING level logs by {@link NetworkException}
   * while {@link SSPParameterWorker} tries to send a push msg,
   * which are intentionally caused many times in {@link #testPushNetworkExceptionAndResend()}.
   */
  @Rule
  private TestRule pushResendWatcher = new EnforceLoggingLevelRule("testPushNetworkExceptionAndResend",
      SSPParameterWorker.class.getName(), Level.SEVERE);

  /**
   * Tests whether worker correctly resend the push operation, when network exception happens.
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
  @Category(IntensiveTest.class)
  public void testPullTimeoutAndRetry()
      throws NetworkException, InterruptedException, TimeoutException, ExecutionException, InjectionException {
    prepare(ParameterWorkerTestUtil.PULL_RETRY_TIMEOUT_MS);
    ParameterWorkerTestUtil.pullTimeoutAndRetry(parameterWorker, workerHandler, mockSender);
  }

  /**
   * Tests whether worker correctly checks and handles data staleness when it receives a request for stale data.
   */
  @Test(timeout = 10000)
  public void testDataStalenessCheck() throws NetworkException, InterruptedException, InjectionException {
    prepare(TIMEOUT_NO_RETRY);

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();
    final ExecutorService executorService =
        ParameterWorkerTestUtil.startPullReplyingThreads(pullKeyToReplyQueue, workerHandler);
    ParameterWorkerTestUtil.setupSenderToEnqueuePullOps(pullKeyToReplyQueue, mockSender);

    final int numberOfKeys = 3;

    // mockSender's sendPullMsg method should be called 'the number of keys' times to pull all the data from servers.
    for (int i = 0; i < numberOfKeys; i++) {
      parameterWorker.pull(i);
    }
    verify(mockSender, times(numberOfKeys)).sendPullMsg(anyString(), anyObject(), anyInt(), any(TraceInfo.class));

    // The number of times sendPullMsg() call shouldn't be changed.
    // Since all the values associated with those keys have been already fetched from servers.
    for (int i = 0; i < numberOfKeys; i++) {
      parameterWorker.pull(i);
    }
    verify(mockSender, times(numberOfKeys)).sendPullMsg(anyString(), anyObject(), anyInt(), any(TraceInfo.class));

    // Now we increase the worker clock until it gets beyond the staleness bound.
    // As a result, all the cached data is going to get stale.
    final int oldWorkerClock = workerClock.getWorkerClock();
    final int deltaWorkerClock = STALENESS_BOUND + 1;
    for (int i = 0; i < deltaWorkerClock; i++) {
      workerClock.clock();
    }
    assertEquals(oldWorkerClock + deltaWorkerClock, workerClock.getWorkerClock());

    // To prevent thread-blocking during pull operations, we increase the global minimum clock once.
    // After increasing it the worker clock will get within the staleness bound again.
    final int oldGlobalMinimumClock = workerClock.getGlobalMinimumClock();
    final int deltaGlobalMinClock = 1;
    final byte[] initClockMsgData =
        codec.encode(ClockManager.getBroadcastMinClockMessage(oldGlobalMinimumClock + deltaGlobalMinClock));
    mockMasterSideCentCommMsgSender.send(ClockManager.AGGREGATION_CLIENT_NAME, WORKER_ID, initClockMsgData);
    assertEquals(workerClock.getWorkerClock(), workerClock.getGlobalMinimumClock() + STALENESS_BOUND);

    // The following for statement makes the number of times sendPullMsg() call increased by the number of keys.
    // Since all the data in worker cache have been stale,
    // so parameter worker should fetch fresh data from servers for all the keys.
    for (int i = 0; i < numberOfKeys; i++) {
      parameterWorker.pull(i);
    }
    verify(mockSender, times(2 * numberOfKeys)).sendPullMsg(anyString(), anyObject(), anyInt(), any(TraceInfo.class));

    executorService.shutdownNow();
  }

  /**
   * Tests whether the staleness condition coordinates workers correctly.
   * When worker threads request pull operations, they are blocked or released according to their staleness condition.
   */
  @Test(timeout = 30000)
  @Category(IntensiveTest.class)
  public void testWorkerStalenessCheck() throws NetworkException, InterruptedException, BrokenBarrierException,
      InjectionException {
    prepare(TIMEOUT_NO_RETRY);

    final BlockingQueue<Pair<EncodedKey<Integer>, Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();
    final ExecutorService executorService =
        ParameterWorkerTestUtil.startPullReplyingThreads(pullKeyToReplyQueue, workerHandler);
    ParameterWorkerTestUtil.setupSenderToEnqueuePullOps(pullKeyToReplyQueue, mockSender);

    final int numOfThreads = 3;
    final int key = 0;
    final long timeoutInMilliSeconds = 10000;
    final int cyclicBarrierSize = numOfThreads + 1;
    final CyclicBarrier barrier = new CyclicBarrier(cyclicBarrierSize);
    final Runnable[] threads = new Runnable[numOfThreads];

    final class WorkerStalenessCheckThread implements Runnable {
      private final CyclicBarrier cyclicBarrier;

      WorkerStalenessCheckThread(final CyclicBarrier barrier) {
        this.cyclicBarrier = barrier;
      }

      @Override
      public void run() {
        parameterWorker.pull(key);
        try {
          // If a thread is waiting at the barrier, that means,
          // it finishes its pull request without being blocked or it has been released.
          cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          fail("Exception occurred while waiting: " + e.getMessage());
        }
      }
    }

    workerClock.initialize();

    // When the worker clock is within the staleness bound, worker threads are not blocked for pull requests.
    while (workerClock.getWorkerClock() <= workerClock.getGlobalMinimumClock() + STALENESS_BOUND) {
      barrier.reset();

      for (int i = 0; i < numOfThreads; i++) {
        threads[i] = new WorkerStalenessCheckThread(barrier);
      }
      ThreadUtils.runConcurrently(threads);

      // The following await() call can release the barrier, only if all threads have returned immediately
      // after pull requests (See WorkerStalenessCheckThread.run()).
      barrier.await();
      workerClock.clock();
    }

    // At this moment, worker clock is beyond the staleness bound from global minimum clock.
    // So these threads below should be blocked during pull requests.
    assertTrue(workerClock.getWorkerClock() > workerClock.getGlobalMinimumClock() + STALENESS_BOUND);

    barrier.reset();
    for (int i = 0; i < numOfThreads; i++) {
      threads[i] = new WorkerStalenessCheckThread(barrier);
    }
    ThreadUtils.runConcurrently(threads);

    // Since all threads are blocked by pull, there should be no thread who is waiting on the cyclic barrier.
    Thread.sleep(timeoutInMilliSeconds);
    assertEquals(barrier.getNumberWaiting(), 0);

    // Manipulate the global minimum clock to make the worker clock get inside of the bound,
    // which releases the blocked threads from pull requests.
    final int oldGlobalMinimumClock = workerClock.getGlobalMinimumClock();
    final int deltaClock = 1;
    final byte[] initClockMsgData =
        codec.encode(ClockManager.getBroadcastMinClockMessage(oldGlobalMinimumClock + deltaClock));
    mockMasterSideCentCommMsgSender.send(ClockManager.AGGREGATION_CLIENT_NAME, WORKER_ID, initClockMsgData);

    assertEquals(oldGlobalMinimumClock + deltaClock, workerClock.getGlobalMinimumClock());
    assertTrue(workerClock.getWorkerClock() <= workerClock.getGlobalMinimumClock() + STALENESS_BOUND);

    // All the threads now wait at the barrier after finishing their pull requests.
    barrier.await();

    executorService.shutdownNow();
  }

  /**
   * Test that the {@link SSPParameterWorker#invalidateAll()} method invalidates all caches
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
    final SSPParameterWorker<Integer, Integer, Integer> sspParameterWorker =
        (SSPParameterWorker<Integer, Integer, Integer>) parameterWorker;

    pool.submit(() -> {
      for (int pull = 0; pull < numPulls; ++pull) {
        sspParameterWorker.pull(0);
        sspParameterWorker.invalidateAll();
      }
      countDownLatch.countDown();
    });
    pool.shutdown();

    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    sspParameterWorker.close(ParameterWorkerTestUtil.CLOSE_TIMEOUT);

    assertTrue(ParameterWorkerTestUtil.MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(mockSender, times(numPulls)).sendPullMsg(anyString(), anyObject(), anyInt(), any(TraceInfo.class));

    executorService.shutdownNow();
  }

  private CentCommMsg getTestCentCommMsg(final String senderId, final byte[] data) {
    return CentCommMsg.newBuilder()
        .setSourceId(senderId)
        .setClientClassName(ClockManager.AGGREGATION_CLIENT_NAME)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
}

