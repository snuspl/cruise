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
import edu.snu.cay.utils.EnforceLoggingLevelRule;
import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.services.ps.avro.AvroClockMsg;
import edu.snu.cay.services.ps.avro.ClockMsgType;
import edu.snu.cay.services.ps.driver.impl.ClockManager;
import edu.snu.cay.services.ps.ns.ClockMsgCodec;
import edu.snu.cay.services.ps.worker.parameters.Staleness;
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
import java.util.concurrent.atomic.AtomicInteger;

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
  private final int initialWorkerClock = 10;
  private final int initialGlobalMinimumClock = 10;
  private final int staleness = 5;

  private ParameterWorkerTestUtil testUtil;
  private SSPParameterWorker<Integer, Integer, Integer> parameterWorker;
  private WorkerHandler<Integer, Integer, Integer> workerHandler;
  private WorkerMsgSender<Integer, Integer> mockSender;
  private AggregationSlave mockAggregationSlave;
  private AggregationMaster mockAggregationMaster;
  private ClockMsgCodec codec;
  private SSPWorkerClock.MessageHandler sspWorkerClockMessageHandler;
  private AtomicInteger numberOfTickMsgCalls;
  private SSPWorkerClock sspWorkerClock;
  private AtomicInteger numberOfPullRequests;

  @Before
  public void setup() throws InjectionException, NetworkException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ServerId.class, "ServerId")
        .bindNamedParameter(WorkerQueueSize.class, Integer.toString(WORKER_QUEUE_SIZE))
        .bindNamedParameter(ParameterWorkerNumThreads.class, Integer.toString(WORKER_NUM_THREADS))
        .bindNamedParameter(PullRetryTimeoutMs.class, Long.toString(ParameterWorkerTestUtil.PULL_RETRY_TIMEOUT_MS))
        .bindNamedParameter(PSParameters.KeyCodecName.class, SerializableCodec.class)
        .bindNamedParameter(Staleness.class, Integer.toString(staleness))
        .bindImplementation(ParameterWorker.class, SSPParameterWorker.class)
        .bindImplementation(WorkerHandler.class, SSPParameterWorker.class)
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    this.testUtil = new ParameterWorkerTestUtil();
    this.numberOfPullRequests = new AtomicInteger(0);
    mockSender = mock(WorkerMsgSender.class);

    injector.bindVolatileInstance(WorkerMsgSender.class, mockSender);
    injector.bindVolatileInstance(ParameterUpdater.class, mock(ParameterUpdater.class));
    injector.bindVolatileInstance(ServerResolver.class, mock(ServerResolver.class));

    // pull messages to asynchronous parameter worker should return values s.t. key == value
    doAnswer(invocationOnMock -> {
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];
        workerHandler.processPullReply(encodedKey.getKey(), encodedKey.getKey());
        numberOfPullRequests.incrementAndGet();
        return null;
      }).when(mockSender).sendPullMsg(anyString(), anyObject());

    this.mockAggregationSlave = mock(AggregationSlave.class);
    injector.bindVolatileInstance(AggregationSlave.class, this.mockAggregationSlave);
    this.mockAggregationMaster = mock(AggregationMaster.class);
    injector.bindVolatileInstance(AggregationMaster.class, this.mockAggregationMaster);
    this.sspWorkerClock = injector.getInstance(SSPWorkerClock.class);

    this.sspWorkerClockMessageHandler = injector.getInstance(SSPWorkerClock.MessageHandler.class);
    this.codec = injector.getInstance(ClockMsgCodec.class);
    this.numberOfTickMsgCalls = new AtomicInteger(0);

    // It may be needed to define aggregation master/slave mockers' actions for send() operation.
    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final AvroClockMsg sendMsg = codec.decode(data);

        if (sendMsg.getType() == ClockMsgType.RequestInitClockMsg) {
          final AvroClockMsg initClockMsg =
              ClockManager.getReplyInitialClockMessage(initialGlobalMinimumClock, initialWorkerClock);
          final byte[] replyData = codec.encode(initClockMsg);
          final AggregationMessage aggregationMessage = getTestAggregationMessage("worker", replyData);
          sspWorkerClockMessageHandler.onNext(aggregationMessage);
        } else if (sendMsg.getType() == ClockMsgType.TickMsg) {
          numberOfTickMsgCalls.incrementAndGet();
        }
        return null;
      }).when(mockAggregationSlave).send(anyString(), anyObject());


    parameterWorker = (SSPParameterWorker) injector.getInstance(ParameterWorker.class);
    workerHandler = injector.getInstance(WorkerHandler.class);
    this.sspWorkerClock.initialize();
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
   * <p>
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
   * <p>
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
   * <p>
   * {@code numPullThreads} threads are generated, each sending {@code numPullPerThread} pulls.
   * For each pull, {@code numKeysPerPull} keys are selected, based on the thread index and the pull count.
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
   * <p>
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
   * Tests whether worker correctly checks and handles data staleness when it receives a request for stale data.
   */
  @Test
  public void testDataStalenessCheck() throws NetworkException, InterruptedException {
    int i;
    final int numberOfKeys = 3;

    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(2, byte[].class);
        final AggregationMessage aggregationMessage = getTestAggregationMessage("worker", data);
        sspWorkerClockMessageHandler.onNext(aggregationMessage);
        return null;
      }).when(mockAggregationMaster).send(anyString(), anyString(), anyObject());

    for (i = 0; i < (2 * numberOfKeys); i++) {
      parameterWorker.pull((i % numberOfKeys));
    }

    assertEquals(initialGlobalMinimumClock, sspWorkerClock.getGlobalMinimumClock());

    final byte[] data =
        codec.encode(ClockManager.getBroadcastMinClockMessage(initialGlobalMinimumClock + (2 * staleness)));
    mockAggregationMaster.send(ClockManager.AGGREGATION_CLIENT_NAME, "worker", data);

    assertEquals(initialGlobalMinimumClock + (2 * staleness), sspWorkerClock.getGlobalMinimumClock());

    for (i = 0; i < numberOfKeys; i++) {
      parameterWorker.pull(i);
    }

    assertEquals(2 * numberOfKeys, numberOfPullRequests.get());
  }

  /**
   * Test that the {@link SSPParameterWorker#invalidateAll()} method invalidate all caches
   * so that new pull messages must be issued for each pull request.
   */
  @Test
  public void testInvalidateAll()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    invalidateAllSSP(parameterWorker);
  }

  private void invalidateAllSSP(final SSPParameterWorker worker)
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

  private AggregationMessage getTestAggregationMessage(final String workerId, final byte[] data) {
    return AggregationMessage.newBuilder()
        .setSourceId(workerId)
        .setClientClassName(ClockManager.AGGREGATION_CLIENT_NAME)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
}

