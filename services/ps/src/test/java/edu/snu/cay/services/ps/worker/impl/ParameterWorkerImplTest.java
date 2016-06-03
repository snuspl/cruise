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
import edu.snu.cay.services.ps.worker.parameters.ParameterWorkerNumThreads;
import edu.snu.cay.services.ps.worker.parameters.WorkerExpireTimeout;
import edu.snu.cay.services.ps.worker.parameters.WorkerQueueSize;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link ParameterWorkerImpl}.
 */
public final class ParameterWorkerImplTest {
  private static final Integer PUSH_VALUE = 1;
  private static final String MSG_THREADS_SHOULD_FINISH = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_THREADS_SHOULD_NOT_FINISH = "threads have finished but should not";
  private static final String MSG_RESULT_ASSERTION = "threads received incorrect values";

  private final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

  private ParameterWorkerImpl<Integer, Integer, Integer> worker;
  private AsyncWorkerHandlerImpl<Integer, Integer> handler;
  private WorkerMsgSender<Integer, Integer> mockSender;


  @Before
  public void setup() throws InjectionException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ServerId.class, "ServerId")
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    mockSender = mock(WorkerMsgSender.class);
    injector.bindVolatileInstance(WorkerMsgSender.class, mockSender);
    injector.bindVolatileInstance(ParameterUpdater.class, mock(ParameterUpdater.class));
    injector.bindVolatileInstance(ServerResolver.class, mock(ServerResolver.class));
    injector.bindVolatileParameter(PSParameters.KeyCodecName.class, new IntegerCodec());
    injector.bindVolatileParameter(WorkerQueueSize.class, 2500);
    injector.bindVolatileParameter(ParameterWorkerNumThreads.class, 2);
    injector.bindVolatileParameter(WorkerExpireTimeout.class, 60000L);

    doAnswer(invocationOnMock -> {
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];
        handler.processReply(encodedKey.getKey(), encodedKey.getKey());
        return null;
      }).when(mockSender).sendPullMsg(anyString(), anyObject());

    worker = injector.getInstance(ParameterWorkerImpl.class);
    handler = injector.getInstance(AsyncWorkerHandlerImpl.class);
  }

  /**
   * Test that {@link ParameterWorkerImpl#close()} does indeed block further operations from being processed.
   */
  @Test
  public void testClose() throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    worker.close();

    pool.submit((Runnable) () -> {
        worker.pull(0);
        countDownLatch.countDown();
      });
    pool.shutdown();

    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    assertFalse(MSG_THREADS_SHOULD_NOT_FINISH, allThreadsFinished);
  }

  /**
   * Test the thread safety of {@link ParameterWorkerImpl} by
   * creating multiple threads that try to push values to the server using {@link ParameterWorkerImpl}.
   */
  @Test
  public void testMultiThreadPush() throws InterruptedException {
    final int numPushThreads = 8;
    final int numPushPerThread = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(numPushThreads);
    final Runnable[] threads = new Runnable[numPushThreads];

    for (int index = 0; index < numPushThreads; ++index) {
      final int key = index % 4;
      threads[index] = () -> {
        for (int push = 0; push < numPushPerThread; ++push) {
          worker.push(key, PUSH_VALUE);
        }
        countDownLatch.countDown();
      };
    }

    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    worker.close();

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(mockSender, times(numPushThreads * numPushPerThread)).sendPushMsg(anyString(), anyObject(), eq(PUSH_VALUE));
  }

  /**
   * Test the thread safety of {@link ParameterWorkerImpl} by
   * creating multiple threads that try to pull values from the server using {@link ParameterWorkerImpl}.
   */
  @Test
  public void testMultiThreadPull() throws InterruptedException {
    final int numPullThreads = 8;
    final int numPullPerThread = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];

    for (int index = 0; index < numPullThreads; ++index) {
      final int key = index % 4;
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
    worker.close();

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());
  }

  /**
   * Test the thread safety of {@link ParameterWorkerImpl} by
   * creating multiple threads that try to pull several values from the server using {@link ParameterWorkerImpl}.
   */
  @Test
  public void testMultiThreadMultiKeyPull() throws InterruptedException {
    final int numPullThreads = 8;
    final int numPullPerThread = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];

    for (int index = 0; index < numPullThreads; ++index) {
      final int threadIndex = index;

      threads[index] = () -> {
        for (int pull = 0; pull < numPullPerThread; ++pull) {
          final List<Integer> keyList = new ArrayList<>(3);
          keyList.add((threadIndex + pull) % 4);
          keyList.add((threadIndex + pull + 1));
          keyList.add((threadIndex + pull + 2) % 4);

          final List<Integer> vals = worker.pull(keyList);
          for (int listIndex = 0; listIndex < 3; ++listIndex) {
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
    worker.close();

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());
  }

  /**
   * Test that the {@link ParameterWorkerImpl#invalidateAll()} method invalidates all caches
   * so that new pull messages must be issued for each pull request.
   */
  @Test
  public void testInvalidateAll() throws InterruptedException {
    final int numPulls = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    pool.submit((Runnable) () -> {
        for (int pull = 0; pull < numPulls; ++pull) {
          worker.pull(0);
          worker.invalidateAll();
        }
        countDownLatch.countDown();
      });
    pool.shutdown();

    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    worker.close();

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(mockSender, times(numPulls)).sendPullMsg(anyString(), anyObject());
  }

  private final class IntegerCodec implements Codec<Integer> {
    @Override
    public Integer decode(final byte[] bytes) {
      final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      return byteBuffer.getInt();
    }

    @Override
    public byte[] encode(final Integer integer) {
      final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
      byteBuffer.putInt(integer);
      return byteBuffer.array();
    }
  }
}
