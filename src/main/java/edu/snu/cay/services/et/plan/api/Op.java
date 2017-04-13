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
package edu.snu.cay.services.et.plan.api;

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.plan.impl.ETPlan;

/**
 * An operation of ET, which composes {@link ETPlan}.
 */
public interface Op {

  /**
   * Executes operation with a given {@link ETMaster}.
   * @param etMaster an interface for manipulating executor, table, and task of ET
   * @return a {@link ListenableFuture} that completes when execution is finished
   * @throws PlanOpExecutionException when exception happens while executing a plan
   */
  ListenableFuture<?> execute(ETMaster etMaster) throws PlanOpExecutionException;
}
