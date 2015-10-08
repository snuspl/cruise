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

import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Driver start handler used for EM add integration test.
 * REEF evaluator requestor does not allow requests before driver start event occurs,
 * so use this handler to satisfy this constraint.
 * Manually call EM add to request for {@code numAdd} evaluators using {@code numThreads} threads.
 */
public final class AddTestStartHandler implements EventHandler<StartTime> {
  private static final Logger LOG = Logger.getLogger(AddTestStartHandler.class.getName());
  private final ElasticMemory elasticMemory;
  private final int numAdd;
  private final int numThreads;

  @Inject
  private AddTestStartHandler(final ElasticMemory elasticMemory,
                              @Parameter(AddIntegrationTest.AddEvalNumber.class) final int numAdd,
                              @Parameter(AddIntegrationTest.AddThreadNumber.class) final int numThreads) {
    this.elasticMemory = elasticMemory;
    this.numAdd = numAdd;
    this.numThreads = numThreads;
  }

  @Override
  public void onNext(final StartTime startTime) {
    final Runnable[] threads = new Runnable[numThreads];
    final CountDownLatch countDownLatch = new CountDownLatch(numAdd);

    // Evenly distribute numAdd requests to numThreads threads.
    final int addsPerThread = numAdd / numThreads;
    int remainder = numAdd % numThreads;
    for (int i = 0; i < numThreads; i++) {
      if (remainder > 0) {
        threads[i] = new AddThread(elasticMemory, countDownLatch, addsPerThread + 1);
        remainder--;
      } else {
        threads[i] = new AddThread(elasticMemory, countDownLatch, addsPerThread);
      }
    }
    boolean addFinished = false;

    try {
      ThreadUtils.runConcurrently(threads);
      addFinished = countDownLatch.await(100, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      addFinished = false;
    } finally {
      LOG.info("Test result: " + addFinished);
      if (!addFinished) {
        throw new RuntimeException("Test failed.");
      }
    }
  }

  final class AddThread implements Runnable {
    private final ElasticMemory elasticMemory;
    private final CountDownLatch countDownLatch;
    private final int addsPerThread;

    AddThread(final ElasticMemory elasticMemory, final CountDownLatch countDownLatch, final int addsPerThread) {
      this.countDownLatch = countDownLatch;
      this.elasticMemory = elasticMemory;
      this.addsPerThread = addsPerThread;
    }

    @Override
    public void run() {
      elasticMemory.add(addsPerThread, 128, 1,
          //Do nothing, close the activeContext
          new EventHandler<ActiveContext>() {
            @Override
            public void onNext(final ActiveContext activeContext) {
              LOG.info("EM add completed. " + activeContext);
              countDownLatch.countDown();
              activeContext.close();
            }
          });
    }
  }
}
