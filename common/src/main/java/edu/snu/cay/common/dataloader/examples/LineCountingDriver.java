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
import javax.xml.bind.DatatypeConverter;
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
  private final AtomicInteger taskIDCounter = new AtomicInteger(0);
  private final AtomicInteger fileCounter = new AtomicInteger(0);
  private final List<String> fileList;
  private final List<ActiveContext> contextList = new ArrayList<>();

  private final int numSplits;
  private final ArrayList<HdfsSplitInfo[]> hdfsSplitInfoList;

  private final HdfsSplitInfoSerializer.HdfsSplitInfoCodec codec = new HdfsSplitInfoSerializer.HdfsSplitInfoCodec();

  @Inject
  private LineCountingDriver(final EvaluatorRequestor evalRequestor,
                             @Parameter(Inputs.class) final Set<String> inputs,
                             @Parameter(Parameters.Splits.class) final int numSplits) {
    this.evalRequestor = evalRequestor;
    this.fileList = new ArrayList<>(inputs);

    // launch evaluators as many as the number of splits and then every evaluator loads one split
    this.numSplits = numSplits;
    this.hdfsSplitInfoList = buildHdfsSplitInfoList();
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
    private final AtomicInteger evalCounter = new AtomicInteger(0);
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int evalIdx = taskIDCounter.getAndIncrement();
      final HdfsSplitInfo splitToLoad = getSplitToLoad(evalCounter.getAndIncrement());
      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, TASK_PREFIX + evalIdx)
          .set(TaskConfiguration.TASK, LineCountingTask.class)
          .set(TaskConfiguration.MEMENTO, encodeHdfsSplitInfo(splitToLoad))
          .build();
      allocatedEvaluator.submitTask(taskConf);
    }
  }

  private HdfsSplitInfo getSplitToLoad(final int index) {
    final HdfsSplitInfo[] fileToLoad = hdfsSplitInfoList.get(fileCounter.get());
    return fileToLoad[index];
  }

  private String encodeHdfsSplitInfo(final HdfsSplitInfo splitToLoad) {
    return DatatypeConverter.printBase64Binary(codec.encode(splitToLoad));
  }

  private ArrayList<HdfsSplitInfo[]> buildHdfsSplitInfoList() {
    final ArrayList<HdfsSplitInfo[]> list = new ArrayList<>();
    for (int i = 0; i < fileList.size(); i++) {
      final HdfsSplitInfo[] splitInfoArray = HdfsSplitManager.getSplits(
              fileList.get(i), TextInputFormat.class.getName(), numSplits);
      list.add(splitInfoArray);
    }
    return list;
  }

  /**
   * A handler of TaskMessage that reports the counted number of lines in loaded files.
   */

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    private final AtomicInteger lineCnt = new AtomicInteger(0);
    private final AtomicInteger taskCnt = new AtomicInteger(0);
    @Override
    public void onNext(final CompletedTask task) {
      final byte[] retBytes = task.get();
      if (retBytes == null) {
        return;
      }
      final String retStr = new String(retBytes, StandardCharsets.UTF_8);
      final int currCnt = Integer.parseInt(retStr);
      lineCnt.addAndGet(currCnt);
      LOG.log(Level.INFO, "file : " + fileCounter.get() + " Get Line Count is : " + retStr);
      contextList.add(task.getActiveContext());

      if (taskCnt.incrementAndGet() >= numSplits) {
        System.out.println("Total Line Count in " + fileList.get(fileCounter.get()) + ": " + lineCnt.get());
        taskCnt.set(0);
        lineCnt.set(0);
        if (fileCounter.incrementAndGet() >= fileList.size()) {
          contextList.forEach(ActiveContext::close);
        } else {
          for (int i = 0; i < contextList.size(); i++) {
            final HdfsSplitInfo splitToLoad = getSplitToLoad(i);
            final Configuration taskConf = TaskConfiguration.CONF
                    .set(TaskConfiguration.IDENTIFIER, TASK_PREFIX + taskIDCounter.getAndIncrement())
                    .set(TaskConfiguration.TASK, LineCountingTask.class)
                    .set(TaskConfiguration.MEMENTO, encodeHdfsSplitInfo(splitToLoad))
                    .build();
            contextList.get(i).submitTask(taskConf);
          }
          contextList.clear();
        }
      }

    }
  }

  @NamedParameter(doc = "A list of file or directory to read input data from",
                  short_name = "inputs")
  final class Inputs implements Name<Set<String>> {
  }
}


