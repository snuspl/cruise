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

import org.apache.reef.annotations.audience.EvaluatorSide;

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
   * Report its progress to {@link ProgressTracker}.
   * @param epochIdx a current processing epoch index
   */
  void report(final int epochIdx) {
    msgSender.sendProgressMsg(epochIdx);
  }
}
