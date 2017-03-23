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
package edu.snu.cay.services.shuffle.evaluator;

import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.utils.test.IntensiveTests;
import org.apache.reef.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Testing thread-safety of ControlMessageSynchronizer.
 */
public final class ControlMessageSynchronizerTest {

  private static final Logger LOG = Logger.getLogger(ControlMessageSynchronizerTest.class.getName());

  private static final long TIMEOUT = 600000;
  private static final int FIRST_MESSAGE = 0;
  private static final int SECOND_MESSAGE = 1;
  private static final int THIRD_MESSAGE = 2;
  private ControlMessageSynchronizer synchronizer;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    synchronizer = new ControlMessageSynchronizer();
  }

  /**
   * Test ControlMessageSynchronizer.resetLatch().
   *
   * The main threads reset all types of control message until entire threads
   * receive prescribed number of messages.
   */
  @Test
  @Category(IntensiveTests.class)
  public void testMultipleResetWithManyMessageTypes() throws Exception {
    LOG.log(Level.INFO, name.getMethodName());

    final int threadNum = 50;
    final int messageNum = 200;
    final Random rand = new Random();
    final CountDownLatch waitForFinishing = new CountDownLatch(threadNum);
    final ExecutorService executor = Executors.newCachedThreadPool();

    for (int i = 0; i < threadNum; i++) {
      executor.submit(new Runnable() {

        @Override
        public void run() {
          for (int i = 0; i < messageNum; i++) {
            final Optional<ShuffleControlMessage> controlMessage;
            if (i % 3 == 0) {
              controlMessage = synchronizer.waitOnLatch(FIRST_MESSAGE, TIMEOUT);
            } else if (i % 3 == 1) {
              controlMessage = synchronizer.waitOnLatch(SECOND_MESSAGE, TIMEOUT);
            } else {
              controlMessage = synchronizer.waitOnLatch(THIRD_MESSAGE, TIMEOUT);
            }

            assert controlMessage.isPresent();

            try {
              Thread.sleep(rand.nextInt(50) + 30);
            } catch (final InterruptedException e) {
              LOG.log(Level.WARNING, "An unexpected InterruptedException occurred : {0}", e);
              throw new RuntimeException(e);
            }
          }

          waitForFinishing.countDown();
        }
      });
    }

    executor.shutdown();

    while (waitForFinishing.getCount() != 0) {
      Thread.sleep(rand.nextInt(50) + 10);
      synchronizer.resetLatch(new ShuffleControlMessage(FIRST_MESSAGE, null));
      synchronizer.resetLatch(new ShuffleControlMessage(SECOND_MESSAGE, null));
      synchronizer.resetLatch(new ShuffleControlMessage(THIRD_MESSAGE, null));
    }

    LOG.log(Level.INFO, "Finished");
  }

  /**
   * Test ControlMessageSynchronizer.closeLatch() and reopenLatch().
   *
   * 1. All threads wait for a control message.
   * 2. The latch is closed so that the threads are notified.
   * 3. The threads test that waitOnLatch returns Optional.empty if the latch was closed.
   * 4. The threads wait for that the latch is re-opened.
   * 5. Finally the threads wait for a control message.
   * 6. Reset the latch to test that re-opened latch works well.
   * 7. The test is finished if all threads are closed successfully.
   */
  @Test
  public void testCloseAndReopenLatch() throws Exception {
    LOG.log(Level.INFO, name.getMethodName());

    final int threadNum = 50;
    final CountDownLatch waitForStarting = new CountDownLatch(threadNum);
    final CountDownLatch waitForTestingClosedState = new CountDownLatch(threadNum);
    final CountDownLatch notifyLatchOpened = new CountDownLatch(1);
    final CountDownLatch waitForLastReset = new CountDownLatch(threadNum);
    final CountDownLatch waitForFinishing = new CountDownLatch(threadNum);

    final ExecutorService executor = Executors.newCachedThreadPool();

    for (int i = 0; i < threadNum; i++) {
      executor.submit(new Runnable() {

        @Override
        public void run() {
          waitForStarting.countDown();

          // The current thread will be notified when the latch is closed
          assert synchronizer.waitOnLatch(FIRST_MESSAGE, TIMEOUT).isPresent();

          for (int i = 0; i < 100; i++) {
            final Optional<ShuffleControlMessage> controlMessage = synchronizer.waitOnLatch(FIRST_MESSAGE, TIMEOUT);

            // The synchronizer returns Optional.empty if the latch was closed
            assert controlMessage.isPresent();
          }

          waitForTestingClosedState.countDown();

          try {
            notifyLatchOpened.await();
          } catch (final InterruptedException e) {
            LOG.log(Level.WARNING, "An unexpected InterruptedException occurred : {0}", e);
            throw new RuntimeException(e);
          }

          waitForLastReset.countDown();

          assert synchronizer.waitOnLatch(FIRST_MESSAGE, TIMEOUT).isPresent();

          waitForFinishing.countDown();
        }
      });
    }

    executor.shutdown();

    LOG.log(Level.INFO, "Wait for that threads are initialized");
    waitForStarting.await();

    Thread.sleep(50);
    LOG.log(Level.INFO, "Close the latch");
    synchronizer.closeLatch(new ShuffleControlMessage(FIRST_MESSAGE, null));

    waitForTestingClosedState.await();

    LOG.log(Level.INFO, "Open the latch");
    synchronizer.reopenLatch(FIRST_MESSAGE);

    notifyLatchOpened.countDown();

    waitForLastReset.await();

    Thread.sleep(500);
    LOG.log(Level.INFO, "Reset the latch");
    synchronizer.resetLatch(new ShuffleControlMessage(FIRST_MESSAGE, null));

    waitForFinishing.await();
    LOG.log(Level.INFO, "Finished");
  }
}
