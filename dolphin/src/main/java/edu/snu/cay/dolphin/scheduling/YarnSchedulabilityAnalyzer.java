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
package edu.snu.cay.dolphin.scheduling;

import javax.inject.Inject;

// TODO #92: This class should be implemented and used as a part of changes to support Gang Scheduling in Yarn.
/**
 * Not yet implemented, so always returns isSchedulable as true.
 * A future implementation should take into account total Yarn resources and
 * queue quotas to determine if Gang Scheduling for dolphin is feasible.
 */
public final class YarnSchedulabilityAnalyzer implements SchedulabilityAnalyzer {

  @Inject
  private YarnSchedulabilityAnalyzer() {
  }

  /**
   * @return true
   */
  @Override
  public boolean isSchedulable() {
    return true;
  }
}
