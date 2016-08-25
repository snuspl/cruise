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
import edu.snu.cay.services.ps.worker.api.WorkerClock;
import edu.snu.cay.services.ps.worker.parameters.ParameterWorkerNumThreads;
import edu.snu.cay.services.ps.worker.parameters.PullRetryTimeoutMs;
import edu.snu.cay.services.ps.worker.parameters.WorkerQueueSize;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import edu.snu.cay.utils.EnforceLoggingLevelRule;
import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.services.ps.avro.AvroClockMsg;
import edu.snu.cay.services.ps.avro.ClockMsgType;
import edu.snu.cay.services.ps.driver.impl.ClockManager;
import edu.snu.cay.services.ps.ns.ClockMsgCodec;
import edu.snu.cay.services.ps.worker.parameters.StalenessBound;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.IdentifierFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.logging.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link SSPParameterWorker}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerMsgSender.class, AggregationSlave.class, AggregationMaster.class})
public final class SSPParameterWorkerTest {
  private static final int WORKER_QUEUE_SIZE = 2500;
  private static final int WORKER_NUM_THREADS = 2;
  private static final int INIT_GLOBAL_MIN_CLOCK = 10;
  private static final int INIT_WORKER_CLOCK = INIT_GLOBAL_MIN_CLOCK;
  private static final int STALENESS_BOUND = 5;
  private static final String WORKER_ID = "worker";
  private ParameterWorkerTestUtil testUtil;
  private ParameterWorker<Integer, Integer, Integer> parameterWorker;
  private WorkerHandler<Integer, Integer, Integer> workerHandler;
  private WorkerMsgSender<Integer, Integer> mockSender;
  private AggregationSlave mockAggregationSlave;
  private AggregationMaster mockAggregationMaster;
  private ClockMsgCodec codec;
  private SSPWorkerClock.MessageHandler sspWorkerClockMessageHandler;
  private WorkerClock workerClock;

  @Before
  public void setup() throws InjectionException, NetworkException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ServerId.class, "ServerId")
        .bindNamedParameter(WorkerQueueSize.class, Integer.toString(WORKER_QUEUE_SIZE))
        .bindNamedParameter(ParameterWorkerNumThreads.class, Integer.toString(WORKER_NUM_THREADS))
        .bindNamedParameter(PullRetryTimeoutMs.class, Long.toString(ParameterWorkerTestUtil.PULL_RETRY_TIMEOUT_MS))
        .bindNamedParameter(PSParameters.KeyCodecName.class, SerializableCodec.class)
        .bindNamedParameter(StalenessBound.class, Integer.toString(STALENESS_BOUND))
        .bindImplementation(ParameterWorker.class, SSPParameterWorker.class)
        .bindImplementation(WorkerHandler.class, SSPParameterWorker.class)
        .bindImplementation(WorkerClock.class, SSPWorkerClock.class)
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    this.testUtil = new ParameterWorkerTestUtil();
    this.mockSender = mock(WorkerMsgSender.class);
    this.mockAggregationSlave = mock(AggregationSlave.class);
    this.mockAggregationMaster = mock(AggregationMaster.class);

    injector.bindVolatileInstance(WorkerMsgSender.class, this.mockSender);
    injector.bindVolatileInstance(ParameterUpdater.class, mock(ParameterUpdater.class));
    injector.bindVolatileInstance(ServerResolver.class, mock(ServerResolver.class));
    injector.bindVolatileInstance(AggregationSlave.class, this.mockAggregationSlave);
    injector.bindVolatileInstance(AggregationMaster.class, this.mockAggregationMaster);

    this.workerClock = injector.getInstance(WorkerClock.class);
    this.sspWorkerClockMessageHandler = injector.getInstance(SSPWorkerClock.MessageHandler.class);
    this.codec = injector.getInstance(ClockMsgCodec.class);
    this.parameterWorker = injector.getInstance(ParameterWorker.class);
    this.workerHandler = injector.getInstance(WorkerHandler.class);

    // pull messages to asynchronous parameter worker should return values s.t. key == value
    doAnswer(invocationOnMock -> {
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];
        workerHandler.processPullReply(encodedKey.getKey(), encodedKey.getKey());
        return null;
      }).when(mockSender).sendPullMsg(anyString(), anyObject());

    // This is a definition of the message receiving part of SSP worker clock message handler
    // When it receives a broadcast message for updating the global minimum clock.
    doAnswer(invocation -> {
        final byte[] initClockMsgData = invocation.getArgumentAt(2, byte[].class);
        final AggregationMessage aggregationMessage = getTestAggregationMessage(WORKER_ID, initClockMsgData);
        sspWorkerClockMessageHandler.onNext(aggregationMessage);
        return null;
      }).when(mockAggregationMaster).send(anyString(), anyString(), anyObject());

    // This is a definition of the message receiving part of SSP worker clock message handler
    // when it receives a response from the driver after sending an initial clock request.
    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final AvroClockMsg sendMsg = codec.decode(data);

        if (sendMsg.getType() == ClockMsgType.RequestInitClockMsg) {
          final AvroClockMsg initClockMsg =
              ClockManager.getReplyInitialClockMessage(INIT_GLOBAL_MIN_CLOCK, INIT_WORKER_CLOCK);
          final byte[] replyData = codec.encode(initClockMsg);
          final AggregationMessage aggregationMessage = getTestAggregationMessage(WORKER_ID, replyData);
          sspWorkerClockMessageHandler.onNext(aggregationMessage);
        }
        return null;
      }).when(mockAggregationSlave).send(anyString(), anyObject());

    // SSP worker clock should be initialized, after the 'send' action of the aggregation slave mocker is defined.
    this.workerClock.initialize();
  }

  /**
   * Test that {@link SSPParameterWorker#close(long)} does indeed block further operations from being processed.
   */
  @Test
  public void testClose() throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    testUtil.close(parameterWorker);
  }

  /**
   * Test the thread safety of {@link SSPParameterWorker} by
   * creating multiple threads that try to push values to the server using {@link SSPParameterWorker}.
   * {@code numPushThreads} threads are generated, each sending {@code numPushPerThread} pushes.
   */
  @Test
  public void testMultiThreadPush()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    testUtil.multiThreadPush(parameterWorker, mockSender);
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
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    testUtil.multiThreadPull(parameterWorker);
  }

  /**
   * Test the thread safety of {@link SSPParameterWorker} by
   * creating multiple threads that try to pull several values from the server using {@link SSPParameterWorker}.
   */
  @Test
  public void testMultiThreadMultiKeyPull() throws InterruptedException, TimeoutException,
      ExecutionException, NetworkException {
    testUtil.multiThreadMultiKeyPull(parameterWorker);
  }

  /**
   * Rule for suppressing massive INFO level logs in {@link AsyncParameterWorker#processPullReject},
   * which are intentionally called many times in {@link #testPullReject}.
   */
  @Rule
  private TestRule watcher = new EnforceLoggingLevelRule("testPullReject",
      SSPParameterWorker.class.getName(), Level.WARNING);

  /**
   * Test the correct handling of pull rejects by {@link SSPParameterWorker},
   * creating multiple threads that try to pull values from the server
   * using {@link SSPParameterWorker}.
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
   * Tests whether worker correctly checks and handles data staleness when it receives a request for stale data.
   */
  @Test
  public void testDataStalenessCheck() throws NetworkException, InterruptedException {
    final int numberOfKeys = 3;

    // mockSender's sendPullMsg method should be called 'the number of keys' times to pull all the data from servers.
    for (int i = 0; i < numberOfKeys; i++) {
      parameterWorker.pull(i);
    }
    verify(mockSender, times(numberOfKeys)).sendPullMsg(anyString(), anyObject());

    // The number of times sendPullMsg() call shouldn't be changed.
    // Since all the values associated with those keys have been already fetched from servers.
    for (int i = 0; i < numberOfKeys; i++) {
      parameterWorker.pull(i);
    }
    verify(mockSender, times(numberOfKeys)).sendPullMsg(anyString(), anyObject());

    // Now we manipulate the global minimum clock to make all the data stale.
    final int oldGlobalMinimumClock = workerClock.getGlobalMinimumClock();
    final int deltaClock = STALENESS_BOUND + 1;
    final byte[] initClockMsgData =
        codec.encode(ClockManager.getBroadcastMinClockMessage(oldGlobalMinimumClock + deltaClock));
    mockAggregationMaster.send(ClockManager.AGGREGATION_CLIENT_NAME, WORKER_ID, initClockMsgData);

    assertEquals(oldGlobalMinimumClock + deltaClock, workerClock.getGlobalMinimumClock());

    // The following for statement makes the number of times sendPullMsg() call increased by the number of keys.
    // Since global minimum clock is now increased by more than staleness bound
    // and all the data in worker cache have now been stale, so parameter worker should fetch fresh data from servers.
    for (int i = 0; i < numberOfKeys; i++) {
      parameterWorker.pull(i);
    }
    verify(mockSender, times(2 * numberOfKeys)).sendPullMsg(anyString(), anyObject());
  }

  // This test case is to test whether the worker staleness check routine works properly
  // to block/release threads who request pull operations with respect to worker staleness condition.
  @Test(timeout = 30000)
  public void testWorkerStalenessCheck() throws NetworkException, InterruptedException, BrokenBarrierException {
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
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
          throw new RuntimeException(e);
        }
      }
    }

    workerClock.initialize();

    // Test to verify whether threads aren't blocked and make progresses for pull requests
    // when the worker clock is in staleness bound.
    while (workerClock.getWorkerClock() <= workerClock.getGlobalMinimumClock() + STALENESS_BOUND) {
      barrier.reset();
      assertEquals(barrier.getNumberWaiting(), 0);
      for (int i = 0; i < numOfThreads; i++) {
        threads[i] = new WorkerStalenessCheckThread(barrier);
      }
      ThreadUtils.runConcurrently(threads);

      // Check whether threads aren't blocked and return immediately after pull requests.
      barrier.await();
      assertEquals(barrier.getNumberWaiting(), 0);
      workerClock.clock();
    }

    // At this moment, worker clock is outside the staleness bound from global minimum clock.
    // So these threads below should be blocked during pull requests.
    assertTrue(workerClock.getWorkerClock() > workerClock.getGlobalMinimumClock() + STALENESS_BOUND);

    barrier.reset();
    for (int i = 0; i < numOfThreads; i++) {
      threads[i] = new WorkerStalenessCheckThread(barrier);
    }
    ThreadUtils.runConcurrently(threads);

    // There should never be a thread waiting on the cyclic barrier after its pull request.
    // Since they will be blocked until the worker clock satisfies the worker staleness condition.
    Thread.sleep(timeoutInMilliSeconds);
    assertEquals(barrier.getNumberWaiting(), 0);

    // Manipulate the global minimum clock to make the worker clock get inside of the bound,
    // which means the blocked threads will be released and process pull requests.
    final int oldGlobalMinimumClock = workerClock.getGlobalMinimumClock();
    final int deltaClock = 1;
    final byte[] initClockMsgData =
        codec.encode(ClockManager.getBroadcastMinClockMessage(oldGlobalMinimumClock + deltaClock));
    mockAggregationMaster.send(ClockManager.AGGREGATION_CLIENT_NAME, WORKER_ID, initClockMsgData);

    assertEquals(oldGlobalMinimumClock + deltaClock, workerClock.getGlobalMinimumClock());
    assertTrue(workerClock.getWorkerClock() <= workerClock.getGlobalMinimumClock() + STALENESS_BOUND);

    // All the threads now should wait at the barrier after finishing pull requests.
    barrier.await();
  }

  /**
   * Test that the {@link SSPParameterWorker#invalidateAll()} method invalidates all caches
   * so that new pull messages must be issued for each pull request.
   */
  @Test
  public void invalidateAll()
      throws InterruptedException, ExecutionException, TimeoutException, NetworkException {
    final int numPulls = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    final SSPParameterWorker sspParameterWorker = (SSPParameterWorker) parameterWorker;

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
    verify(mockSender, times(numPulls)).sendPullMsg(anyString(), anyObject());
  }

  private AggregationMessage getTestAggregationMessage(final String workerId, final byte[] data) {
    return AggregationMessage.newBuilder()
        .setSourceId(workerId)
        .setClientClassName(ClockManager.AGGREGATION_CLIENT_NAME)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
}

