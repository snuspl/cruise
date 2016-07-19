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
import edu.snu.cay.services.ps.worker.api.AsyncWorkerHandler;
import edu.snu.cay.services.ps.worker.parameters.ParameterWorkerNumThreads;
import edu.snu.cay.services.ps.worker.parameters.WorkerQueueSize;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link ParameterWorkerImpl}.
 */
public final class ParameterWorkerImplTest {
  private static final long CLOSE_TIMEOUT = 5000;
  private static final int WORKER_QUEUE_SIZE = 2500;
  private static final int WORKER_NUM_THREADS = 2;
  private static final String MSG_THREADS_SHOULD_FINISH = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_THREADS_SHOULD_NOT_FINISH = "threads have finished but should not";
  private static final String MSG_RESULT_ASSERTION = "threads received incorrect values";

  private ParameterWorkerImpl<Integer, Integer, Integer> worker;
  private AsyncWorkerHandler<Integer, Integer, Integer> handler;
  private WorkerMsgSender<Integer, Integer> mockSender;

  @Before
  public void setup() throws InjectionException, NetworkException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ServerId.class, "ServerId")
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    mockSender = mock(WorkerMsgSender.class);
    injector.bindVolatileInstance(WorkerMsgSender.class, mockSender);
    injector.bindVolatileInstance(ParameterUpdater.class, mock(ParameterUpdater.class));
    injector.bindVolatileInstance(ServerResolver.class, mock(ServerResolver.class));
    injector.bindVolatileParameter(PSParameters.KeyCodecName.class, new IntegerCodec());
    injector.bindVolatileParameter(WorkerQueueSize.class, WORKER_QUEUE_SIZE);
    injector.bindVolatileParameter(ParameterWorkerNumThreads.class, WORKER_NUM_THREADS);

    // pull messages should return values s.t. key == value
    doAnswer(invocationOnMock -> {
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];
        handler.processPullReply(encodedKey.getKey(), encodedKey.getKey());
        return null;
      }).when(mockSender).sendPullMsg(anyString(), anyObject());

    worker = injector.getInstance(ParameterWorkerImpl.class);
    handler = injector.getInstance(AsyncWorkerHandlerImpl.class);
  }

  /**
   * Test that {@link ParameterWorkerImpl#close(long)} does indeed block further operations from being processed.
   */
  @Test
  public void testClose() throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final ExecutorService pool = Executors.newSingleThreadExecutor();

    worker.close(CLOSE_TIMEOUT);

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
   *
   * {@code numPushThreads} threads are generated, each sending {@code numPushPerThread} pushes.
   */
  @Test
  public void testMultiThreadPush()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPushThreads = 8;
    final int numKeys = 4;
    final int numPushPerThread = 1000;
    final int pushValue = 1;
    final CountDownLatch countDownLatch = new CountDownLatch(numPushThreads);
    final Runnable[] threads = new Runnable[numPushThreads];

    for (int index = 0; index < numPushThreads; ++index) {
      final int key = index % numKeys;
      threads[index] = () -> {
        for (int push = 0; push < numPushPerThread; ++push) {
          worker.push(key, pushValue);
        }
        countDownLatch.countDown();
      };
    }

    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);
    worker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    verify(mockSender, times(numPushThreads * numPushPerThread)).sendPushMsg(anyString(), anyObject(), eq(pushValue));
  }

  /**
   * Test the thread safety of {@link ParameterWorkerImpl} by
   * creating multiple threads that try to pull values from the server using {@link ParameterWorkerImpl}.
   *
   * {@code numPullThreads} threads are generated, each sending {@code numPullPerThread} pulls.
   * Due to the cache, {@code sender.sendPullMsg()} may not be invoked as many times as {@code worker.pull()} is called.
   * Thus, we verify the validity of the result by simply checking whether pulled values are as expected or not.
   */
  @Test
  public void testMultiThreadPull()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numKeys = 4;
    final int numPullPerThread = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

    for (int index = 0; index < numPullThreads; ++index) {
      final int key = index % numKeys;
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
    worker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());
  }

  /**
   * Test the thread safety of {@link ParameterWorkerImpl} by
   * creating multiple threads that try to pull several values from the server using {@link ParameterWorkerImpl}.
   *
   * {@code numPullThreads} threads are generated, each sending {@code numPullPerThread} pulls.
   * For each pull, {@code numKeysPerPull} keys are selected, based on the thread index and the pull count.
   */
  @Test
  public void testMultiThreadMultiKeyPull() throws InterruptedException, TimeoutException,
      ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numKeys = 4;
    final int numPullPerThread = 1000;
    final int numKeysPerPull = 3;
    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

    for (int index = 0; index < numPullThreads; ++index) {
      final int threadIndex = index;

      threads[index] = () -> {
        for (int pull = 0; pull < numPullPerThread; ++pull) {
          final List<Integer> keyList = new ArrayList<>(numKeysPerPull);
          for (int keyIndex = 0; keyIndex < numKeysPerPull; ++keyIndex) {
            keyList.add((threadIndex + pull + keyIndex) % numKeys);
          }

          final List<Integer> vals = worker.pull(keyList);
          for (int listIndex = 0; listIndex < keyList.size(); ++listIndex) {
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
    worker.close(CLOSE_TIMEOUT);

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());
  }

  /**
   * Test the correct handling of pull rejects by {@link ParameterWorkerImpl},
   * creating multiple threads that try to pull values from the server using {@link ParameterWorkerImpl}.
   *
   * {@code numPullThreads} threads are generated, each sending {@code numPullPerThread} pulls.
   * To guarantee that {@code sender.sendPullMsg()} should be invoked as many times as {@code worker.pull()} is called,
   * this test use different keys for each pull.
   */
  @Test
  public void testPullReject()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
    final int numPullThreads = 8;
    final int numPullPerThread = 1000;
    final int numRejectPerKey = 3; // should be smaller than MAX_RETRY_COUNT in ParameterWorkerImpl

    final Map<Integer, AtomicInteger> keyToNumPullCounter = new HashMap<>();
    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean correctResultReturned = new AtomicBoolean(true);

    final BlockingQueue<EncodedKey<Integer>> pullKeyToReplyQueue = new LinkedBlockingQueue<>();

    // start a thread that process pull requests from the pullKeyToReplyQueue
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            final EncodedKey<Integer> encodedKey = pullKeyToReplyQueue.take();

            if (!keyToNumPullCounter.containsKey(encodedKey.getKey())) {
              keyToNumPullCounter.put(encodedKey.getKey(), new AtomicInteger(0));
            }

            final int numAnswerForTheKey = keyToNumPullCounter.get(encodedKey.getKey()).getAndIncrement();

            if (numAnswerForTheKey < numRejectPerKey) {
              handler.processPullReject(encodedKey.getKey());
            } else {
              // pull messages should return values s.t. key == value
              handler.processPullReply(encodedKey.getKey(), encodedKey.getKey());
            }

          } catch (final InterruptedException e) {
            break; // it's an intended InterruptedException to quit the thread
          }
        }
      }
    });

    // put key of pull msgs into pullKeyToReplyQueue
    doAnswer(invocationOnMock -> {
        final EncodedKey<Integer> encodedKey = (EncodedKey) invocationOnMock.getArguments()[1];

        pullKeyToReplyQueue.put(encodedKey);

        return null;
      }).when(mockSender).sendPullMsg(anyString(), anyObject());

    for (int index = 0; index < numPullThreads; ++index) {
      final int baseKey = index * numPullPerThread;
      threads[index] = () -> {
        for (int pull = 0; pull < numPullPerThread; pull++) {
          final int key = baseKey + pull;
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
    worker.close(CLOSE_TIMEOUT);

    executorService.shutdownNow(); // it will interrupt all running threads

    assertTrue(MSG_THREADS_SHOULD_FINISH, allThreadsFinished);
    assertTrue(MSG_RESULT_ASSERTION, correctResultReturned.get());
    verify(mockSender, times(numPullPerThread * numPullThreads * (numRejectPerKey + 1)))
        .sendPullMsg(anyString(), anyObject());
  }

  /**
   * Test that the {@link ParameterWorkerImpl#invalidateAll()} method invalidates all caches
   * so that new pull messages must be issued for each pull request.
   */
  @Test
  public void testInvalidateAll()
      throws InterruptedException, TimeoutException, ExecutionException, NetworkException {
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
    worker.close(CLOSE_TIMEOUT);

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
