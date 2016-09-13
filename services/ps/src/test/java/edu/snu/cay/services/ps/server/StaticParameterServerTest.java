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
import edu.snu.cay.services.ps.PSParameters;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.common.resolver.SingleNodeServerResolver;
import edu.snu.cay.services.ps.common.resolver.ServerId;
import edu.snu.cay.services.ps.examples.add.IntegerCodec;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.common.parameters.NumPartitions;
import edu.snu.cay.services.ps.server.api.ServerSideReplySender;
import edu.snu.cay.services.ps.server.impl.fixed.StaticParameterServer;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.htrace.TraceInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicMarkableReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static edu.snu.cay.services.ps.common.Constants.SERVER_ID_PREFIX;


/**
 * Tests for {@link StaticParameterServer}.
 */
public final class StaticParameterServerTest {
  private static final long CLOSE_TIMEOUT = 10000;
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_RESULT_ASSERTION = "final result of concurrent pushes and pulls";
  private static final String WORKER_ID = "WORKER";
  private static final int REQUEST_ID = 0;
  private static final TraceInfo EMPTY_TRACE = null;

  private StaticParameterServer<Integer, Integer, Integer> server;
  private ServerSideReplySender<Integer, Integer, Integer> mockSender;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(ServerResolver.class, SingleNodeServerResolver.class)
        .bindNamedParameter(ServerId.class, SERVER_ID_PREFIX + 0)
        .bindNamedParameter(EndpointId.class, SERVER_ID_PREFIX + 0)
        .bindNamedParameter(PSParameters.KeyCodecName.class, IntegerCodec.class)
        .bindNamedParameter(PSParameters.ValueCodecName.class, IntegerCodec.class)
        .bindNamedParameter(PSParameters.PreValueCodecName.class, IntegerCodec.class)
        .bindNamedParameter(NumPartitions.class, "4")
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileInstance(ServerSideReplySender.class, mock(ServerSideReplySender.class));
    injector.bindVolatileInstance(MetricsHandler.class, Mockito.mock(MetricsHandler.class));
    injector.bindVolatileInstance(MetricsMsgSender.class, Mockito.mock(MetricsMsgSender.class));
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

    mockSender = injector.getInstance(ServerSideReplySender.class);
    server = injector.getInstance(StaticParameterServer.class);
  }

  /**
   * Test the performance of {@link StaticParameterServer} by
   * running threads that push values to and pull values from the server, concurrently.
   */
  @Test(timeout = 100000)
  public void testMultiThreadPushPull() throws InterruptedException {
    final int numPushThreads = 8;
    final int numPushes = 100000;
    final int numPullThreads = 8;
    final int numPulls = 100000;
    final CountDownLatch countDownLatch = new CountDownLatch(numPushThreads + numPullThreads);
    final Runnable[] threads = new Runnable[numPushThreads + numPullThreads];

    for (int threadIndex = 0; threadIndex < numPushThreads; threadIndex++) {
      final int threadId = threadIndex;
      threads[threadIndex] = new Runnable() {
        @Override
        public void run() {
          for (int index = 0; index < numPushes; index++) {
            // each thread increments the server's value by 1 per push
            final int key = threadId;
            server.push(key, 1, WORKER_ID, key); // Just use key as hash for this test.
          }
          countDownLatch.countDown();
        }
      };
    }

    for (int threadIndex = 0; threadIndex < numPullThreads; threadIndex++) {
      final int threadId = threadIndex;
      threads[threadIndex + numPushThreads] = new Runnable() {
        @Override
        public void run() {
          for (int index = 0; index < numPulls; index++) {
            final int key = threadId;
            server.pull(key, WORKER_ID, key, REQUEST_ID, EMPTY_TRACE); // Just use key as hash for this test.
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
      final int key = threadIndex;
      server.pull(key, WORKER_ID, key, REQUEST_ID, EMPTY_TRACE); // Just use key as hash for this test.

      waitForOps();
      while (!replayValue.isMarked()) {
        Thread.sleep(5);
      }

      assertEquals(MSG_RESULT_ASSERTION, numPushes, (int) replayValue.getReference());
      replayValue.set(null, false); // reset
    }
  }

  private void waitForOps() throws InterruptedException {
    int opsPending = server.opsPending();
    while (opsPending > 0) {
      System.out.println("Ops Pending: " + opsPending);
      Thread.sleep(5);
      opsPending = server.opsPending();
    }
  }

  @Test
  public void testClose() throws InterruptedException, ExecutionException, TimeoutException {
    final int numPulls = 5;

    doAnswer(invocation -> {
        // sleep to guarantee the queue not empty when closing server
        Thread.sleep(1000);
        return null;
      }).when(mockSender).sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(), any(TraceInfo.class));

    for (int i = 0; i < numPulls; i++) {
      final int key = i;
      server.pull(key, WORKER_ID, key, 0, EMPTY_TRACE);
    }

    // closing server should guarantee all the queued operations to be processed, if time allows
    server.close(CLOSE_TIMEOUT);
    verify(mockSender, times(numPulls)).sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(),
        any(TraceInfo.class));

    // server should not process further operations after being closed
    server.pull(0, WORKER_ID, 0, 0, EMPTY_TRACE);
    verify(mockSender, times(numPulls)).sendPullReplyMsg(anyString(), anyInt(), anyInt(), anyInt(), anyLong(),
        any(TraceInfo.class));
  }
}
