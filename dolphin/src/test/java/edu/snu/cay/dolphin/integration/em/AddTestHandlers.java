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
package edu.snu.cay.dolphin.integration.em;

import edu.snu.cay.dolphin.parameters.DesiredSplits;
import edu.snu.cay.services.dataloader.DataLoader;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF event handlers for EM add integration test.
 */
@Unit
final class AddTestHandlers {
  private static final Logger LOG = Logger.getLogger(AddTestHandlers.class.getName());
  private final ElasticMemory elasticMemory;
  private final DataLoader dataLoader;
  private final int numAdd;
  private final int numThreads;
  private final CountDownLatch allocationCounter;
  private final CountDownLatch callbackCounter;
  private final int dataLoaderRequestNum;
  private final Set<ActiveContext> activeContextSet;
  private RuntimeException runtimeException;

  @Inject
  private AddTestHandlers(final ElasticMemory elasticMemory,
                          final DataLoader dataLoader,
                          @Parameter(AddIntegrationTest.AddEvalNumber.class) final int numAdd,
                          @Parameter(AddIntegrationTest.AddThreadNumber.class) final int numThreads,
                          @Parameter(DesiredSplits.class) final int numSplits) {
    this.elasticMemory = elasticMemory;
    this.dataLoader = dataLoader;
    this.numAdd = numAdd;
    this.numThreads = numThreads;
    this.allocationCounter = new CountDownLatch(numAdd);
    this.callbackCounter = new CountDownLatch(numAdd);
    this.dataLoaderRequestNum = numSplits + 1;
    this.activeContextSet  = Collections.newSetFromMap(new ConcurrentHashMap<ActiveContext, Boolean>());
  }

  /**
   * Driver start handler.
   * REEF evaluator requestor does not allow requests before driver start event occurs,
   * so use this handler to satisfy this constraint.
   * Manually call EM add to request for {@code numAdd} evaluators using {@code numThreads} threads.
   * Use an additional thread to wait for {@code allocationCounter} and {@code callbackCounter}.
   */
  final class AddTestStartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "Add integration test allocating {0} evaluators", numAdd);
      final Runnable[] threads = new Runnable[numThreads + 1];

      // Evenly distribute numAdd requests to numThreads threads.
      // We spawn these separate threads for calling EM.add() because this thread may be blocking other
      // EventHandler<StartTime> threads from running.
      final int addsPerThread = numAdd / numThreads;
      int remainder = numAdd % numThreads;
      for (int i = 0; i < numThreads; i++) {
        if (remainder > 0) {
          threads[i] = new AddThread(addsPerThread + 1);
          remainder--;
        } else {
          threads[i] = new AddThread(addsPerThread);
        }
      }

      // The CountdownLatch.await() calls must be independent of REEF events,
      // thus we create a separate thread for this instead of putting this code in an event handler.
      threads[numThreads] = new Runnable() {
        @Override
        public void run() {
          try {
            LOG.log(Level.INFO, "Waiting...");
            final boolean allocationFinished = allocationCounter.await(100, TimeUnit.SECONDS);
            final boolean callbackFinished = callbackCounter.await(100, TimeUnit.SECONDS);
            LOG.log(Level.INFO, "Outstanding EM evaluator allocation: {0}", allocationCounter.getCount());
            LOG.log(Level.INFO, "Outstanding EM add callback: {0}", callbackCounter.getCount());
            if (!(allocationFinished && callbackFinished)) {
              runtimeException = new RuntimeException(String.format(
                  "Test failed. %d/%d evaluators allocated, %d/%d callbacks triggered.",
                  numAdd - allocationCounter.getCount(), numAdd, numAdd - callbackCounter.getCount(), numAdd));
            }

          } catch (final InterruptedException e) {
            // An exception throw from this thread does not affect the REEF job;
            // we throw this later at the driver stop handler.
            runtimeException = new RuntimeException("Test failed.", e);

          } finally {
            // ActiveContexts are closed all at once to prevent early termination of this REEF job,
            // which leads to the shutdown of this thread.
            for (final ActiveContext activeContext : activeContextSet) {
              activeContext.close();
            }
          }
        }
      };

      try {
        ThreadUtils.runConcurrently(threads);
      } catch (final InterruptedException e) {
        runtimeException = new RuntimeException("Test failed.", e);
      }
      LOG.info("exiting");
    }

    final class AddThread implements Runnable {
      private final int addsPerThread;

      AddThread(final int addsPerThread) {
        this.addsPerThread = addsPerThread;
      }

      @Override
      public void run() {
        // Wait until DataLoader receives evaluators
        // TODO #153: use resource manager to manage all evaluator requests
        while (dataLoader.isDataLoaderRequest()) {
          LOG.fine("Sleeping...");
          try {
            Thread.sleep(1000);
          } catch (final InterruptedException e) {
            runtimeException = new RuntimeException("Test failed.", e);
          }
        }
        elasticMemory.add(addsPerThread, 128, 1,
            // Checks that EM add callback was actually triggered
            new EventHandler<ActiveContext>() {
              @Override
              public void onNext(final ActiveContext activeContext) {
                LOG.log(Level.INFO, "EM add callback for active context {0} triggered successfully.", activeContext);
                callbackCounter.countDown();
                activeContextSet.add(activeContext);
              }
            });
      }
    }
  }

  /**
   * Evaluator allocated handler.
   * Checks that EM add request actually allocates new evaluators.
   */
  final class AddTestEvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    private int dataLoaderEvalCount = 0;

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Evaluator allocated {0}", allocatedEvaluator);
      if (dataLoaderEvalCount < dataLoaderRequestNum) {
        dataLoaderEvalCount++;
      } else {
        LOG.log(Level.INFO, "EM add allocated evaluator {0} successfully.", allocatedEvaluator);
        allocationCounter.countDown();
      }
    }
  }

  /**
   * Driver close handler.
   * Make test fails by throwing runtime exception if there was some problem.
   */
  final class AddTestStopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      if (runtimeException != null) {
        throw runtimeException;
      }
    }
  }
}
