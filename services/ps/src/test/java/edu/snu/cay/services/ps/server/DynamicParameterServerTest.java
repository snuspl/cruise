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

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.*;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.impl.HashBlockResolver;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.services.em.evaluator.impl.singlekey.MemoryStoreImpl;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.ps.ParameterServerParameters;
import edu.snu.cay.services.ps.common.parameters.NumPartitions;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.common.resolver.SingleNodeServerResolver;
import edu.snu.cay.services.ps.common.resolver.ServerId;
import edu.snu.cay.services.ps.examples.add.IntegerCodec;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.api.ServerSideReplySender;
import edu.snu.cay.services.ps.server.impl.dynamic.DynamicParameterServer;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.htrace.HTraceConfiguration;
import org.htrace.Span;
import org.htrace.SpanReceiver;
import org.htrace.TraceInfo;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static edu.snu.cay.services.ps.common.Constants.SERVER_ID_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Tests for {@link DynamicParameterServer}.
 */
public final class DynamicParameterServerTest {
  private static final Integer KEY = 0;
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_RESULT_ASSERTION = "final result of concurrent pushes and pulls";
  private DynamicParameterServer<Integer, Integer, Integer> server;
  private MockServerSideReplySender mockSender;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bind(ServerSideReplySender.class, MockServerSideReplySender.class)
        .bindImplementation(ServerResolver.class, SingleNodeServerResolver.class)
        .bindNamedParameter(ServerId.class, SERVER_ID_PREFIX + 0)
        .bindNamedParameter(EndpointId.class, SERVER_ID_PREFIX + 0)
        .bindNamedParameter(ParameterServerParameters.KeyCodecName.class, IntegerCodec.class)
        .bindNamedParameter(ParameterServerParameters.ValueCodecName.class, IntegerCodec.class)
        .bindNamedParameter(ParameterServerParameters.PreValueCodecName.class, IntegerCodec.class)
        .bindNamedParameter(NumPartitions.class, "4")
        .bindImplementation(MemoryStore.class, MemoryStoreImpl.class)
        .bindImplementation(BlockResolver.class, HashBlockResolver.class)
        .bindImplementation(SpanReceiver.class, MockSpanReceiver.class)
        .bindImplementation(ElasticMemoryMsgSender.class, MockMsgSender.class)
        .bindNamedParameter(KeyCodecName.class, IntegerCodec.class)
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(0))
        .bindNamedParameter(NumStoreThreads.class, "2")
        .bindNamedParameter(NumTotalBlocks.class, "2")
        .bindNamedParameter(NumInitialEvals.class, "1")
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
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
    router.initialize("DUMMY");

    mockSender = injector.getInstance(MockServerSideReplySender.class);
    server = injector.getInstance(DynamicParameterServer.class);
  }

  /**
   * Test the performance of {@link DynamicParameterServer} by
   * running threads that push values to and pull values from the server, concurrently.
   */
  @Test(timeout = 200000)
  public void testMultiThreadPushPull() throws InterruptedException {
    final int numPushThreads = 8;
    final int numPushes = 1000000;
    final int numPullThreads = 8;
    final int numPulls = 1000000;
    final CountDownLatch countDownLatch = new CountDownLatch(numPushThreads + numPullThreads);
    final Runnable[] threads = new Runnable[numPushThreads + numPullThreads];

    for (int threadIndex = 0; threadIndex < numPushThreads; threadIndex++) {
      final int threadId = threadIndex;
      threads[threadIndex] = new Runnable() {
        @Override
        public void run() {
          for (int index = 0; index < numPushes; index++) {
            // each thread increments the server's value by 1 per push
            final int key = KEY + threadId;
            server.push(key, 1, key); // Just use key as hash for this test.
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
            final int key = KEY + threadId;
            server.pull(key, "", key); // Just use key as hash for this test.
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
    for (int threadIndex = 0; threadIndex < numPushThreads; threadIndex++) {
      final int key = KEY + threadIndex;
      server.pull(key, "", key); // Just use key as hash for this test.
      waitForOps();
      assertEquals(MSG_RESULT_ASSERTION, numPushes, mockSender.getLatest());
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

  /**
   * Mocked reply sender to instantiate DynamicParameterServer.
   */
  private static class MockServerSideReplySender
      implements ServerSideReplySender<Integer, Integer> {
    private volatile int latest = -1;

    @Inject
    public MockServerSideReplySender() {
    }

    @Override
    public void sendReplyMsg(final String destId, final Integer key, final Integer value) {
      latest = value;
    }

    public int getLatest() {
      return latest;
    }
  }

  /**
   * Mocked span receiver, which is required to instantiate MemoryStore.
   */
  private static class MockSpanReceiver implements SpanReceiver {
    @Inject
    MockSpanReceiver() {
    }

    @Override
    public void configure(final HTraceConfiguration hTraceConfiguration) {
    }

    @Override
    public void receiveSpan(final Span span) {
    }

    @Override
    public void close() throws IOException {
    }
  }

  /**
   * Mocked message sender that implements ElasticMemoryMsgSender, which is required to instantiate MemoryStore.
   */
  private static final class MockMsgSender implements ElasticMemoryMsgSender {

    @Inject
    private MockMsgSender() {

    }

    @Override
    public void sendRemoteOpMsg(final String origId, final String destId, final DataOpType operationType,
                                final String dataType, final List<KeyRange> dataKeyRanges,
                                final List<KeyValuePair> dataKVPairList, final String operationId,
                                @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendRemoteOpMsg(final String origId, final String destId, final DataOpType operationType,
                                final String dataType, final DataKey dataKey, final DataValue dataValue,
                                final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendRemoteOpResultMsg(final String destId, final List<KeyValuePair> dataKVPairList,
                                      final List<KeyRange> failedRanges, final String operationId,
                                      @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendRemoteOpResultMsg(final String destId, final DataValue dataValue, final boolean isSuccess,
                                      final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendRoutingTableInitReqMsg(@Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingTableInitMsg(final String destId, final List<Integer> blockLocations,
                                        @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingTableUpdateMsg(final String destId, final List<Integer> blocks, final String oldEvalId,
                                          final String newEvalId, @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                            final List<Integer> blocks, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendDataMsg(final String destId, final String dataType,
                            final List<KeyValuePair> keyValuePairs, final int blockId, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendOwnershipMsg(final Optional<String> destId, final String operationId, final String dataType,
                                 final int blockId, final int oldOwnerId, final int newOwnerId,
                                 @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendOwnershipAckMsg(final String operationId, final String dataType, final int blockId,
                                    @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendFailureMsg(final String operationId, final String reason,
                               @Nullable final TraceInfo parentTraceInfo) {
    }
  }
}
