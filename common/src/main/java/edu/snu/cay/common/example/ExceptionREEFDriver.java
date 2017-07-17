/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.common.example;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Exception REEF app.
 */
@Unit
public final class ExceptionREEFDriver {

  private static final Logger LOG = Logger.getLogger(ExceptionREEFDriver.class.getName());

  private final EvaluatorRequestor requestor;

  @Inject
  private ExceptionREEFDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
    LOG.log(Level.FINE, "Instantiated 'ExceptionREEFDriver'");
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      requestor.newRequest()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .submit();
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting ExceptionTask task to AllocatedEvaluator: {0}", allocatedEvaluator);
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "ExceptionREEFTask")
          .set(TaskConfiguration.TASK, ExceptionREEFTask.class)
          .build();
      allocatedEvaluator.submitTask(taskConfiguration);
    }
  }

  public final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      throw new RuntimeException("Task fail", failedEvaluator.getFailedTask().get().getReason().get());
    }
  }
}
