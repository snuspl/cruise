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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.common.centcomm.slave.SlaveSideCentCommMsgSender;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A class for reporting epoch progress to driver.
 */
@EvaluatorSide
public final class ProgressReporter {
  private final String executorId;
  private final String jobId;
  private final SlaveSideCentCommMsgSender msgSender;

  @Inject
  private ProgressReporter(@Parameter(ExecutorIdentifier.class) final String executorId,
                           @Parameter(JobIdentifier.class) final String jobId,
                           final SlaveSideCentCommMsgSender msgSender) {
    this.executorId = executorId;
    this.jobId = jobId;
    this.msgSender = msgSender;
  }

  /**
   * Report its progress to {@link ProgressTracker}.
   * @param epochIdx a current processing epoch index
   */
  public void report(final int epochIdx) {
    final ProgressMsg progressMsg = ProgressMsg.newBuilder()
        .setExecutorId(executorId)
        .setEpochIdx(epochIdx)
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setType(dolphinMsgType.ProgressMsg)
        .setProgressMsg(progressMsg)
        .build();

    msgSender.send(jobId, AvroUtils.toBytes(dolphinMsg, DolphinMsg.class));
  }
}
