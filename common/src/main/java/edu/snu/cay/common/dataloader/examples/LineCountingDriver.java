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
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
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

  private static final String TASK_PREFIX = "LineCountingTask-";
  private static final String CONTEXT_PREFIX = "BaseCtx-";

  private final AtomicInteger taskIdCounter = new AtomicInteger(0);
  private final AtomicInteger ctxIdcounter = new AtomicInteger(0);

  private final EvaluatorRequestor evalRequestor;
  private final AtomicInteger fileCounter = new AtomicInteger(0);
  private final List<String> fileList;

  private final List<ActiveContext> contextList;

  private final int numSplits;
  private final ArrayList<HdfsSplitInfo[]> hdfsSplitInfoList;

  @Inject
  private LineCountingDriver(final EvaluatorRequestor evalRequestor,
                             @Parameter(Inputs.class) final Set<String> inputs,
                             @Parameter(Parameters.Splits.class) final int numSplits) {
    this.evalRequestor = evalRequestor;
    this.fileList = new ArrayList<>(inputs);

    // launch evaluators as many as the number of splits and then every evaluator loads one split
    this.numSplits = numSplits;
    this.hdfsSplitInfoList = buildHdfsSplitInfoList();
    this.contextList = Collections.synchronizedList(new ArrayList<>(numSplits));
  }

  private ArrayList<HdfsSplitInfo[]> buildHdfsSplitInfoList() {
    final ArrayList<HdfsSplitInfo[]> list = new ArrayList<>();
    for (final String aFileList : fileList) {
      final HdfsSplitInfo[] splitInfoArray = HdfsSplitManager.getSplits(
          aFileList, TextInputFormat.class.getName(), numSplits);
      list.add(splitInfoArray);
    }
    return list;
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
      allocatedEvaluator.submitContext(ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, CONTEXT_PREFIX + ctxIdcounter.getAndIncrement())
          .build()
      );
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    private final AtomicInteger activeCtxCounter = new AtomicInteger(0);

    @Override
    public void onNext(final ActiveContext activeContext) {
      final int numActiveCtxs = activeCtxCounter.incrementAndGet();

      contextList.add(activeContext);

      LOG.log(Level.INFO, "Active context: ({0} / {1})", new Object[]{numActiveCtxs, numSplits});
      if (numActiveCtxs == numSplits) {
        final HdfsSplitInfo[] fileSplitsToLoad = hdfsSplitInfoList.get(fileCounter.get());
        submitLineCountingTasks(fileSplitsToLoad);
      }
    }
  }

  private void submitLineCountingTasks(final HdfsSplitInfo[] fileSplitsToload) {
    LOG.log(Level.INFO, "Submit line counting tasks");
    for (int idx = 0; idx < contextList.size(); idx++) {
      final String taskId = TASK_PREFIX + taskIdCounter.getAndIncrement();
      final HdfsSplitInfo fileSplitToload = fileSplitsToload[idx];
      final ActiveContext context = contextList.get(idx);

      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, LineCountingTask.class)
          .set(TaskConfiguration.MEMENTO, encodeHdfsSplitInfo(fileSplitToload))
          .build();

      context.submitTask(taskConf);
    }
  }

  private String encodeHdfsSplitInfo(final HdfsSplitInfo splitToLoad) {
    return HdfsSplitInfoSerializer.serialize(splitToLoad);
  }

  /**
   * A handler of TaskMessage that reports the counted number of lines in loaded files.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    private final AtomicInteger lineCnt = new AtomicInteger(0);
    private final AtomicInteger completedTaskCnt = new AtomicInteger(0);
    @Override
    public void onNext(final CompletedTask task) {
      final byte[] retBytes = task.get();
      if (retBytes == null) {
        return;
      }

      final int retCnt = Integer.parseInt(new String(retBytes, StandardCharsets.UTF_8));
      final int numCompletedTask = completedTaskCnt.incrementAndGet();
      final String filePath = fileList.get(fileCounter.get());

      lineCnt.addAndGet(retCnt);
      LOG.log(Level.INFO, "Number of lines counted by {0} for file {1} is {2}", new Object[]{task.getId(),
          filePath, retCnt});

      LOG.log(Level.INFO, "Completed tasks: ({0} / {1})", new Object[]{numCompletedTask, contextList.size()});

      // when all tasks for a file is completed
      if (numCompletedTask == contextList.size()) {
        System.out.println(String.format("Total Line Count in %s : %d", filePath, lineCnt.get()));

        tryNextFile();
      }
    }

    private void tryNextFile() {
      // if there's no next file
      if (fileCounter.incrementAndGet() >= fileList.size()) {
        contextList.forEach(ActiveContext::close);
        return;
      }

      final HdfsSplitInfo[] fileSplitsToLoad = hdfsSplitInfoList.get(fileCounter.get());
      submitLineCountingTasks(fileSplitsToLoad);

      completedTaskCnt.set(0);
      lineCnt.set(0);
    }
  }

  @NamedParameter(doc = "A list of file or directory to read input data from",
                  short_name = "inputs")
  final class Inputs implements Name<Set<String>> {
  }
}


