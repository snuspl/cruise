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
package edu.snu.spl.cruise.services.et.plan.api;

import edu.snu.spl.cruise.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.spl.cruise.services.et.driver.api.ETMaster;
import edu.snu.spl.cruise.services.et.exceptions.PlanOpExecutionException;
import edu.snu.spl.cruise.services.et.metric.MetricManager;
import edu.snu.spl.cruise.services.et.plan.impl.ETPlan;
import edu.snu.spl.cruise.services.et.plan.impl.OpResult;

import java.util.Map;

/**
 * An operation of ET, which composes {@link ETPlan}.
 */
public interface Op {

  /**
   * Types of operation.
   */
  enum OpType {
    ALLOCATE,
    DEALLOCATE,
    ASSOCIATE,
    UNASSOCIATE,
    SUBSCRIBE,
    UNSUBSCRIBE,
    MOVE,
    CREATE,
    DROP,
    START,
    STOP
  }

  /**
   * @return an identifier of operation
   */
  long getOpId();

  /**
   * @return a type of operation
   */
  OpType getOpType();

  /**
   * Executes operation with a given {@link ETMaster}.
   * @param etMaster an interface for manipulating executor, table, and task of ET
   * @param virtualIdToActualId a mapping of virtual executor id, which is for executors to be allocated
   *                            within this plan, to actual id. AllocateOp should update this map
   *                            and other operations (e.g., Move, Create, Subscribe, Start)
   *                            that can follow AllocateOp should read this map.
   * @return a {@link ListenableFuture} that completes when execution is finished
   * @throws PlanOpExecutionException when exception happens while executing a plan
   */
  ListenableFuture<OpResult> execute(ETMaster etMaster, MetricManager metricManager,
                                     Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException;
}
