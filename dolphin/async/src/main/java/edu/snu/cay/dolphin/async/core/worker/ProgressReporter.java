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
package edu.snu.cay.dolphin.async.core.worker;

import edu.snu.cay.dolphin.async.core.driver.master.BatchProgressTracker;
import edu.snu.cay.dolphin.async.core.driver.master.ProgressTracker;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;

import javax.inject.Inject;

/**
 * A class for reporting epoch progress to driver.
 */
@EvaluatorSide
public final class ProgressReporter {
  private final WorkerSideMsgSender msgSender;

  @Inject
  private ProgressReporter(final WorkerSideMsgSender msgSender) {
    this.msgSender = msgSender;
  }

  /**
   * Report that epoch has started to {@link ProgressTracker}.
   * @param epochIdx a current processing epoch index
   */
  void reportEpochStart(final int epochIdx) throws NetworkException {
    msgSender.sendEpochProgressMsg(epochIdx);
  }

  /**
   * Report that a mini-batch has finished to {@link BatchProgressTracker}.
   * @param miniBatchIdx a processed mini-batch idx
   */
  void reportBatchFinish(final int miniBatchIdx) throws NetworkException {
    msgSender.sendBatchProgressMsg(miniBatchIdx);
  }
}
