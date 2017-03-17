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
 * Driver side for the line counting app that uses the data loader.
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
  private final List<String> filePathList;

  /**
   * The number of splits for each file.
   */
  private final int numSplits;

  /**
   * Each element in a list represents one file.
   */
  private final ArrayList<HdfsSplitInfo[]> hdfsSplitInfoList;

  /**
   * A list of contexts running on evaluators. It lasts during a job.
   * Each context in a list is for maintaining evaluator between multiple tasks.
   */
  private final List<ActiveContext> contextList;

  @Inject
  private LineCountingDriver(final EvaluatorRequestor evalRequestor,
                             @Parameter(Inputs.class) final Set<String> inputs,
                             @Parameter(Parameters.Splits.class) final int numSplits) {
    this.evalRequestor = evalRequestor;
    this.filePathList = new ArrayList<>(inputs);

    this.numSplits = numSplits;
    this.hdfsSplitInfoList = buildHdfsSplitInfosList(filePathList);
    this.contextList = Collections.synchronizedList(new ArrayList<>(numSplits));
  }

  /**
   * Splits the given files and assembles the splits of a single file into an array.
   * @param filePaths a list of file paths
   * @return a list of array of {@link HdfsSplitInfo}, each array contains all splits of a file
   */
  private ArrayList<HdfsSplitInfo[]> buildHdfsSplitInfosList(final List<String> filePaths) {
    final ArrayList<HdfsSplitInfo[]> list = new ArrayList<>();
    for (final String filePath : filePaths) {
      final HdfsSplitInfo[] splitInfoArray = HdfsSplitManager.getSplits(
          filePath, TextInputFormat.class.getName(), numSplits);
      list.add(splitInfoArray);
    }
    return list;
  }

  /**
   * Launches evaluators as many as the number of splits and then every evaluator loads one split of .
   */
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

  /**
   * Submits a context that will last during executing multiple tasks.
   * It is for easily managing the contexts, by explicitly submitting the contexts.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      allocatedEvaluator.submitContext(ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, CONTEXT_PREFIX + ctxIdcounter.getAndIncrement())
          .build()
      );
    }
  }

  /**
   * Builds a list of contexts that we can submit tasks.
   * It submits tasks to the given context after adding {@link ActiveContext} to {@link #contextList}.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    private final AtomicInteger activeCtxCounter = new AtomicInteger(0);

    @Override
    public void onNext(final ActiveContext activeContext) {
      final int activeCtxCnt = activeCtxCounter.incrementAndGet();
      contextList.add(activeContext);
      LOG.log(Level.FINER, "Active context: ({0} / {1})", new Object[]{activeCtxCnt, numSplits});

      // when all contexts become active, submit line counting tasks
      if (activeCtxCnt == numSplits) {
        // we assume that we have at least one file to load
        final HdfsSplitInfo[] fileSplitsToLoad = hdfsSplitInfoList.get(fileCounter.get());
        submitLineCountingTasks(fileSplitsToLoad);
      }
    }
  }

  /**
   * Handles CompletedTask: It sums task return values and makes a total line count of each file.
   * If there are any remaining files, try next file.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    private final AtomicInteger lineCounter = new AtomicInteger(0);
    private final AtomicInteger completedTaskCounter = new AtomicInteger(0);
    @Override
    public void onNext(final CompletedTask task) {
      final byte[] retBytes = task.get();
      if (retBytes == null) {
        return;
      }

      final String filePath = filePathList.get(fileCounter.get());
      final int retCnt = Integer.parseInt(new String(retBytes, StandardCharsets.UTF_8));
      lineCounter.addAndGet(retCnt);
      LOG.log(Level.FINE, "Number of lines counted by {0} for a file {1} is {2}", new Object[]{task.getId(),
          filePath, retCnt});

      final int completedTaskCnt = completedTaskCounter.incrementAndGet();
      LOG.log(Level.FINER, "Completed tasks: ({0} / {1})", new Object[]{completedTaskCnt, contextList.size()});

      // when all tasks for a file is completed
      if (completedTaskCnt == contextList.size()) {
        System.out.println(String.format("Total Line Count in %s : %d", filePath, lineCounter.get()));

        if (!tryToLoadNextFile()) {
          // close contexts, when it finishes all file
          contextList.forEach(ActiveContext::close);
        }
      }
    }

    /**
     * Submits tasks for the next round, if there is a remaining file to count.
     * It sets the state of {@link CompletedTaskHandler}
     * (e.g., {@link #lineCounter}, {@link #completedTaskCounter}) to ready for the new file.
     * @return True when it succeed to take next file
     */
    private boolean tryToLoadNextFile() {
      if (fileCounter.incrementAndGet() >= filePathList.size()) {
        return false;
      }

      final HdfsSplitInfo[] fileSplitsToLoad = hdfsSplitInfoList.get(fileCounter.get());
      submitLineCountingTasks(fileSplitsToLoad);
      completedTaskCounter.set(0);
      lineCounter.set(0);
      return true;
    }
  }

  /**
   * Load next file to contexts.
   * It assigns on split to each context.
   * @param fileSplitsToLoad Array of HdfsSplitInfo from a file.
   */

  private void submitLineCountingTasks(final HdfsSplitInfo[] fileSplitsToLoad) {
    LOG.log(Level.FINER, "Submit line counting tasks");
    for (int idx = 0; idx < contextList.size(); idx++) {

      final String taskId = TASK_PREFIX + taskIdCounter.getAndIncrement();
      final HdfsSplitInfo fileSplitToLoad = fileSplitsToLoad[idx];
      final ActiveContext context = contextList.get(idx);

      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, LineCountingTask.class)
          .set(TaskConfiguration.MEMENTO, encodeHdfsSplitInfo(fileSplitToLoad))
          .build();

      context.submitTask(taskConf);
    }
  }

  private static String encodeHdfsSplitInfo(final HdfsSplitInfo splitToLoad) {
    return HdfsSplitInfoSerializer.serialize(splitToLoad);
  }

  @NamedParameter(doc = "A list of file or directory to read input data from",
                  short_name = "inputs")
  final class Inputs implements Name<Set<String>> {
  }
}


