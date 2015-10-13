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
package edu.snu.cay.services.em.plan.api;

import edu.snu.cay.services.em.plan.impl.LoggingPlanExecutor;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.concurrent.Future;

/**
 * A plan executor interface.
 * Plan execution returns a Future. Executors should start new thread(s) to execute within and update the Future
 * when complete.
 */
@DefaultImplementation(LoggingPlanExecutor.class)
public interface PlanExecutor {

  /**
   * @param plan to execute
   * @return a Future that summarizes a plan execution when it has finished
   */
  Future<PlanResult> execute(Plan plan);
}
