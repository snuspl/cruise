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
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link ParameterWorkerImpl}.
 */
public final class ParameterWorkerImplTest {
  private static final Integer KEY = 0;
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_RESULT_ASSERTION = "threads received null";
  private ParameterWorkerImpl<Integer, Integer, Integer> worker;
  private AsyncWorkerHandlerImpl<Integer, Integer> handler;

  @Before
  public void setup() throws InjectionException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ServerId.class, "ServerId")
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final WorkerMsgSender<Integer, Integer> mockSender = mock(WorkerMsgSender.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        final Thread sendThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              // simulate slow network by purposely sleeping for 5 seconds
              Thread.sleep(5000);
              handler.processReply(KEY, 1);
            } catch (final InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        });

        sendThread.start();
        return null;
      }
    }).when(mockSender).sendPullMsg(anyString(), anyObject());

    final Codec<Integer> integerCodec = new Codec<Integer>() {
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
    };

    injector.bindVolatileInstance(WorkerMsgSender.class, mockSender);
    injector.bindVolatileInstance(ParameterUpdater.class, mock(ParameterUpdater.class));
    injector.bindVolatileInstance(ServerResolver.class, mock(ServerResolver.class));
    injector.bindVolatileParameter(PSParameters.KeyCodecName.class, integerCodec);
    worker = injector.getInstance(ParameterWorkerImpl.class);
    handler = injector.getInstance(AsyncWorkerHandlerImpl.class);
  }

  /**
   * Test the thread safety of {@link ParameterWorkerImpl} by
   * creating multiple threads that try to pull values from the server using {@link ParameterWorkerImpl}.
   */
  @Test
  public void testMultiThreadPull() throws InterruptedException {
    final int numPullThreads = 8;
    final CountDownLatch countDownLatch = new CountDownLatch(numPullThreads);
    final Runnable[] threads = new Runnable[numPullThreads];
    final AtomicBoolean threadReceivedNull = new AtomicBoolean(false);

    for (int index = 0; index < numPullThreads; index++) {
      threads[index] = new Runnable() {
        @Override
        public void run() {
          if (worker.pull(KEY) == null) {
            threadReceivedNull.set(true);
          }
          countDownLatch.countDown();
        }
      };
    }

    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(10, TimeUnit.SECONDS);

    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    assertFalse(MSG_RESULT_ASSERTION, threadReceivedNull.get());
  }
}
