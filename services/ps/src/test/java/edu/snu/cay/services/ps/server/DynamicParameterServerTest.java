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
package edu.snu.cay.services.ps.server;

import edu.snu.cay.common.metric.MetricsHandler;
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.services.em.common.parameters.*;
import edu.snu.cay.services.em.evaluator.api.*;
import edu.snu.cay.services.em.evaluator.impl.HashBlockResolver;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.services.em.evaluator.impl.singlekey.MemoryStoreImpl;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.ps.PSParameters;
import edu.snu.cay.services.ps.examples.add.IntegerCodec;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.api.ServerSideMsgSender;
import edu.snu.cay.services.ps.server.impl.dynamic.DynamicParameterServer;
import edu.snu.cay.services.ps.server.impl.dynamic.EMUpdateFunctionForPS;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.htrace.SpanReceiver;
import org.htrace.TraceInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.logging.Logger;

import static edu.snu.cay.services.ps.common.Constants.SERVER_ID_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link DynamicParameterServer}.
 */
public final class DynamicParameterServerTest {
  private static final Logger LOG = Logger.getLogger(DynamicParameterServerTest.class.getName());
  private static final long CLOSE_TIMEOUT = 20000;

  /**
   * It should be set to prevent server threads not to poll whole operations at once
   * before {@link #testClose()} starts closing server. Otherwise no operation will be rejected.
   */
  private static final int SERVER_QUEUE_SIZE = 10;

  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_RESULT_ASSERTION = "final result of concurrent pushes and pulls";
  private static final String WORKER_ID = "WORKER";
  private static final int REQUEST_ID = 0;
  private static final TraceInfo EMPTY_TRACE = null;

  private static final int NUM_TOTAL_BLOCKS = 2;
  private static final int NUM_TOTAL_STORES = 2;
  private static final int LOCAL_STORE_ID = 0;
  private static final int REMOTE_STORE_ID = 1;

  private DynamicParameterServer<Integer, Integer, Integer> server;
  private ServerSideMsgSender<Integer, Integer, Integer> mockSender;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(EndpointId.class, SERVER_ID_PREFIX + 0)
        .bindNamedParameter(PSParameters.KeyCodecName.class, IntegerCodec.class)
        .bindNamedParameter(PSParameters.ValueCodecName.class, IntegerCodec.class)
        .bindNamedParameter(PSParameters.PreValueCodecName.class, IntegerCodec.class)
        .bindNamedParameter(ServerQueueSize.class, Integer.toString(SERVER_QUEUE_SIZE))
        .bindImplementation(MemoryStore.class, MemoryStoreImpl.class)
        .bindImplementation(EMUpdateFunction.class, EMUpdateFunctionForPS.class)
        .bindImplementation(BlockResolver.class, HashBlockResolver.class)
        .bindNamedParameter(KeyCodecName.class, IntegerCodec.class)
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(LOCAL_STORE_ID))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(NUM_TOTAL_STORES))
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileInstance(RemoteAccessibleMemoryStore.class, mock(RemoteAccessibleMemoryStore.class));
    injector.bindVolatileInstance(ServerSideMsgSender.class, mock(ServerSideMsgSender.class));
    injector.bindVolatileInstance(ElasticMemoryMsgSender.class, mock(ElasticMemoryMsgSender.class));
    injector.bindVolatileInstance(SpanReceiver.class, mock(SpanReceiver.class));
    injector.bindVolatileInstance(MetricsHandler.class, mock(MetricsHandler.class));
    injector.bindVolatileInstance(MetricsMsgSender.class, mock(MetricsMsgSender.class));
    injector.bindVolatileInstance(ParameterUpdater.class, new ParameterUpdater<Integer, Integer, Integer>() {
      @Override
      public Integer process(final Integer key, final Integer preValue) {
        return preValue;
      }

      @Override
      public Integer update(final Integer oldValue, final Integer deltaValue) {
        // simply add the processed value to the original value 
        return oldValue + deltaValue;
      }

      @Override
      public Integer initValue(final Integer key) {
        return 0;
      }
    });

    // EM's router should be initialized explicitly
    final OperationRouter router = injector.getInstance(OperationRouter.class);
    router.triggerInitialization();

    mockSender = injector.getInstance(ServerSideMsgSender.class);
    server = injector.getInstance(DynamicParameterServer.class);
  }

  private DataIdFactory getDataIdFactory(final int storeId) throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(storeId))
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(NUM_TOTAL_STORES))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    return injector.getInstance(DataIdFactory.class);
  }

  /**
   * Test the performance of {@link DynamicParameterServer} by
   * running threads that push values to and pull values from the server, concurrently.
   */
  @Test(timeout = 100000)
  public void testMultiThreadPushPull() throws InterruptedException, TimeoutException, ExecutionException,
      InjectionException, IdGenerationException {
    final int numPushThreads = 8;
    final int numPushes = 100000;
    final int numPullThreads = 8;
    final int numPulls = 100000;
    final CountDownLatch countDownLatch = new CountDownLatch(numPushThreads + numPullThreads);
    final Runnable[] threads = new Runnable[numPushThreads + numPullThreads];
    final DataIdFactory<Long> localDataIdFactory = getDataIdFactory(LOCAL_STORE_ID);

    final List<Long> keys = localDataIdFactory.getIds(Math.max(numPushThreads, numPullThreads));

    for (int threadIndex = 0; threadIndex < numPushThreads; threadIndex++) {
      final int keyIdx = threadIndex;
      threads[threadIndex] = new Runnable() {
        @Override
        public void run() {
          final int key = keys.get(keyIdx).intValue();
          for (int index = 0; index < numPushes; index++) {
            // each thread increments the server's value by 1 per push
            server.push(key, 1, key); // Just use key as hash for this test.
          }
          countDownLatch.countDown();
        }
      };
    }

    for (int threadIndex = 0; threadIndex < numPullThreads; threadIndex++) {
      final int keyIdx = threadIndex;
      threads[threadIndex + numPushThreads] = new Runnable() {
        @Override
        public void run() {
          final int key = keys.get(keyIdx).intValue();
          for (int index = 0; index < numPulls; index++) {
            server.pull(key, WORKER_ID, key, REQUEST_ID, EMPTY_TRACE); // Just use key as hash for this test
          }
          countDownLatch.countDown();
        }
      };
    }

    final long startTime = System.currentTimeMillis();
    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(100, TimeUnit.SECONDS);
    waitForOps();
    final long endTime = System.currentTimeMillis();
    System.out.println("Ops completed in " + (endTime - startTime) + " milliseconds");

    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    verify(mockSender, times(numPulls * numPullThreads))
        .sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(), any(TraceInfo.class));

    final AtomicMarkableReference<Integer> replayValue = new AtomicMarkableReference<>(null, false);
    doAnswer(invocation -> {
        final int value = invocation.getArgumentAt(2, Integer.class);
        replayValue.set(value, true);
        return null;
      }).when(mockSender).sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(), any(TraceInfo.class));

    for (int threadIndex = 0; threadIndex < numPushThreads; threadIndex++) {
      final int key = keys.get(threadIndex).intValue();
      server.pull(key, WORKER_ID, key, REQUEST_ID, EMPTY_TRACE); // Just use key as hash for this test.

      waitForOps();
      while (!replayValue.isMarked()) {
        Thread.sleep(5);
      }

      assertEquals(MSG_RESULT_ASSERTION, numPushes, (int) replayValue.getReference());
      replayValue.set(null, false); // reset
    }
    server.close(CLOSE_TIMEOUT);
  }

  private void waitForOps() throws InterruptedException {
    int opsPending = server.opsPending();
    while (opsPending > 0) {
      System.out.println("Ops Pending: " + opsPending);
      Thread.sleep(5);
      opsPending = server.opsPending();
    }
  }

  @Test(timeout = 10000)
  public void testRedirectPushPull() throws InjectionException, IdGenerationException,
      InterruptedException, ExecutionException, TimeoutException {
    final int numKeys = 10;
    final DataIdFactory<Long> remoteDataIdFactory = getDataIdFactory(REMOTE_STORE_ID);

    final List<Long> remoteKeys = remoteDataIdFactory.getIds(numKeys);

    for (final long longKey : remoteKeys) {
      final int key = (int) longKey;
      server.push(key, 0, key);
      server.pull(key, WORKER_ID, key, 0, EMPTY_TRACE);
    }

    // closing server should guarantee all the queued operations to be processed, if time allows
    server.close(CLOSE_TIMEOUT);

    verify(mockSender, never()).sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(),
        any(TraceInfo.class));
    verify(mockSender, times(numKeys)).sendPullMsg(anyString(), anyString(), anyInt(), anyInt(), any(TraceInfo.class));
    verify(mockSender, times(numKeys)).sendPushMsg(anyString(), anyInt(), anyInt());
  }

  @Test
  public void testClose() throws InterruptedException, ExecutionException, TimeoutException,
      InjectionException, IdGenerationException {
    final int numPulls = 5;
    final DataIdFactory<Long> localDataIdFactory = getDataIdFactory(LOCAL_STORE_ID);

    doAnswer(invocation -> {
        // sleep to guarantee the queue not empty when closing server
        Thread.sleep(1000);
        return null;
      }).when(mockSender).sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(), any(TraceInfo.class));

    for (int i = 0; i < numPulls; i++) {
      final int key = localDataIdFactory.getId().intValue();
      server.pull(key, WORKER_ID, key, REQUEST_ID, EMPTY_TRACE);
    }

    // closing server should guarantee all the queued operations to be processed, if time allows
    server.close(CLOSE_TIMEOUT);
    verify(mockSender, times(numPulls)).sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(),
        any(TraceInfo.class));

    // server should not process further operations after being closed
    server.pull(0, WORKER_ID, 0, REQUEST_ID, EMPTY_TRACE);
    verify(mockSender, times(numPulls)).sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(),
        any(TraceInfo.class));
  }
}
