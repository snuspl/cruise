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
package edu.snu.cay.dolphin.bsp.scheduling;

import edu.snu.cay.common.param.Parameters.LocalRuntimeMaxNumEvaluators;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A Schedulability Analyzer for the Local Runtime.
 * A Dolphin job is schedulable if the max number of evaluators is at least the sum of the
 * number of compute tasks (= number of data loading partitions) and number of controller tasks (=1).
 */
public final class LocalSchedulabilityAnalyzer implements SchedulabilityAnalyzer {
  private final boolean schedulable;

  @Inject
  private LocalSchedulabilityAnalyzer(final DataLoadingService dataLoadingService,
                                      @Parameter(LocalRuntimeMaxNumEvaluators.class) final int maxNumEvaluators) {
    this.schedulable = maxNumEvaluators >= (dataLoadingService.getNumberOfPartitions() + 1);
  }

  @Override
  public boolean isSchedulable() {
    return schedulable;
  }
}
