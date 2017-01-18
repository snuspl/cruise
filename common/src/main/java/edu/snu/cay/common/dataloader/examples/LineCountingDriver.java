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
package edu.snu.cay.common.dataloader.examples;

import edu.snu.cay.common.dataloader.HdfsSplitInfoSerializer;
import edu.snu.cay.common.dataloader.TextInputFormat;
import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitManager;
import edu.snu.cay.common.param.Parameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver side for the line counting that uses the data loader.
 */
@DriverSide
@Unit
public final class LineCountingDriver {
  private static final Logger LOG = Logger.getLogger(LineCountingDriver.class.getName());

  private static final String TASK_PREFIX = "Task-";

  private final EvaluatorRequestor evalRequestor;
  private final AtomicInteger evalCounter = new AtomicInteger(0);

  private final int numSplits;
  private final HdfsSplitInfo[] hdfsSplitInfoArray;

  @Inject
  private LineCountingDriver(final EvaluatorRequestor evalRequestor,
                             @Parameter(Parameters.InputDir.class) final String inputDir,
                             @Parameter(Parameters.Splits.class) final int numSplits) {
    this.evalRequestor = evalRequestor;
    this.numSplits = numSplits;
    this.hdfsSplitInfoArray = HdfsSplitManager.getSplits(inputDir, TextInputFormat.class.getName(), numSplits);
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      evalRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numSplits)
          .setMemory(128)
          .setNumberOfCores(1)
          .build());
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int evalIdx = evalCounter.getAndIncrement();

      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, TASK_PREFIX + evalIdx)
          .set(TaskConfiguration.TASK, LineCountingTask.class)
          .build();

      final String serializedSplitInfo = HdfsSplitInfoSerializer.serialize(hdfsSplitInfoArray[evalIdx]);

      final Configuration dataSplitConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(SerializedSplitInfo.class, serializedSplitInfo)
          .build();

      allocatedEvaluator.submitTask(Configurations.merge(taskConf, dataSplitConf));
    }
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {
    private final AtomicInteger lineCnt = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);

    @Override
    public void onNext(final CompletedTask completedTask) {

      final String taskId = completedTask.getId();
      LOG.log(Level.FINEST, "Completed Task: {0}", taskId);

      final byte[] retBytes = completedTask.get();
      final String retStr = retBytes == null ? "No RetVal" : new String(retBytes, StandardCharsets.UTF_8);
      LOG.log(Level.FINE, "Line count from {0} : {1}", new String[]{taskId, retStr});

      lineCnt.addAndGet(Integer.parseInt(retStr));

      if (completedTasks.incrementAndGet() >= evalCounter.get()) {
        LOG.log(Level.INFO, "Total line count: {0}", lineCnt.get());
      }

      LOG.log(Level.FINEST, "Releasing Context: {0}", completedTask.getActiveContext().getId());
      completedTask.getActiveContext().close();
    }
  }

  @NamedParameter(doc = "Serialized split info")
  static final class SerializedSplitInfo implements Name<String> {

  }
}
