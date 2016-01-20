/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.ps.worker;

import edu.snu.cay.services.ps.driver.impl.ServerId;
import edu.snu.cay.services.ps.worker.api.WorkerSideMsgSender;
import edu.snu.cay.services.ps.worker.impl.SingleNodeParameterWorker;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link SingleNodeParameterWorker}.
 */
public final class SingleNodeParameterWorkerTest {
  private static final Integer KEY = 0;
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_RESULT_ASSERTION = "threads received null";
  private SingleNodeParameterWorker<Integer, Integer, Integer> worker;

  @Before
  public void setup() throws InjectionException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ServerId.class, "ServerId")
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final WorkerSideMsgSender<Integer, Integer> mockSender = mock(WorkerSideMsgSender.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        final Thread sendThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              // simulate slow network by purposely sleeping for 5 seconds
              Thread.sleep(5000);
              worker.processReply(KEY, 1);
            } catch (final InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        });

        sendThread.start();
        return null;
      }
    }).when(mockSender).sendPullMsg(anyString(), anyInt());

    injector.bindVolatileInstance(WorkerSideMsgSender.class, mockSender);
    worker = injector.getInstance(SingleNodeParameterWorker.class);
  }

  /**
   * Test the thread safety of {@link SingleNodeParameterWorker} by
   * creating multiple threads that try to pull values from the server using {@link SingleNodeParameterWorker}.
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
