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
import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitManager;
import edu.snu.cay.common.dataloader.TextInputFormat;
import edu.snu.cay.common.param.Parameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.*;
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

  private final AtomicInteger fileCounter = new AtomicInteger(0);
  private final List<String> inputPathList;

  private final int numEvals;
  private final List<RunningTask> runningTaskList;
  
  private final HdfsSplitInfoSerializer.HdfsSplitInfoCodec codec = new HdfsSplitInfoSerializer.HdfsSplitInfoCodec();

  @Inject
  private LineCountingDriver(final EvaluatorRequestor evalRequestor,
                             @Parameter(Inputs.class) final Set<String> inputs,
                             @Parameter(Parameters.Splits.class) final int numEvals) {
    this.evalRequestor = evalRequestor;
    this.inputPathList = new ArrayList<>(inputs);
    this.numEvals = numEvals;
    this.runningTaskList = Collections.synchronizedList(new ArrayList<>(numEvals));
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      evalRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numEvals)
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
          .set(TaskConfiguration.ON_SEND_MESSAGE, LineCountingTask.class)
          .set(TaskConfiguration.ON_MESSAGE, LineCountingTask.DriverMsgHandler.class)
          .set(TaskConfiguration.ON_CLOSE, LineCountingTask.CloseEventHandler.class)
          .build();

      allocatedEvaluator.submitTask(taskConf);
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      runningTaskList.add(runningTask);

      if (runningTaskList.size() == numEvals) {
        if (!loadNextFileInEvals()) {
          throw new RuntimeException("Has no file to load");
        }
      }
    }
  }

  /**
   * Load a next file in evaluators.
   * @return True when succeed to load a file and False if there's no file to load
   */
  private boolean loadNextFileInEvals() {
    if (fileCounter.get() >= inputPathList.size()) {
      return false;
    }

    final HdfsSplitInfo[] hdfsSplitInfoArray = HdfsSplitManager.getSplits(
        inputPathList.get(fileCounter.getAndIncrement()), TextInputFormat.class.getName(), numEvals);
    for (int evalIdx = 0; evalIdx < numEvals; evalIdx++) {
      runningTaskList.get(evalIdx).send(codec.encode(hdfsSplitInfoArray[evalIdx]));
    }

    return true;
  }

  /**
   * A handler of TaskMessage that reports the counted number of lines in loaded files.
   */
  final class TaskMsgHandler implements EventHandler<TaskMessage> {
    private final AtomicInteger lineCnt = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);

    @Override
    public void onNext(final TaskMessage taskMessage) {
      final String taskId = taskMessage.getId();

      final byte[] retBytes = taskMessage.get();
      if (retBytes == null) {
        LOG.log(Level.FINE, "No line was read by {0}", taskId);
        return;
      }

      final String retStr = new String(retBytes, StandardCharsets.UTF_8);
      LOG.log(Level.FINE, "Line count from {0} : {1}", new String[]{taskId, retStr});

      lineCnt.addAndGet(Integer.parseInt(retStr));

      if (completedTasks.incrementAndGet() >= numEvals) {
        LOG.log(Level.INFO, "FilePath: {0}, Total line count: {1}",
            new Object[]{inputPathList.get(fileCounter.get() - 1), lineCnt.get()});

        lineCnt.set(0);
        completedTasks.set(0);

        // close the tasks when all the file has been loaded
        if (!loadNextFileInEvals()) {
          runningTaskList.forEach(RunningTask::close);
        }
      }
    }
  }

  @NamedParameter(doc = "A list of file or directory to read input data from",
                  short_name = "inputs")
  final class Inputs implements Name<Set<String>> {
  }
}
