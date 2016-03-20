/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.common.parameters.PartitionId;
import edu.snu.cay.services.em.evaluator.api.PartitionFunc;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * OperationRouter that redirects incoming operations to corresponding evaluators.
 * Currently it manages only a local partition.
 */
final class OperationRouter {
  private final int localPartitionId;

  private final PartitionFunc partitionFunc;

  @Inject
  private OperationRouter(final PartitionFunc partitionFunc,
                          @Parameter(PartitionId.class) final int partitionId) {
    this.localPartitionId = partitionId;
    this.partitionFunc = partitionFunc;
  }

  /**
   * Return that a data id is one of local partition or not.
   * @param dataId a id of data
   * @return a boolean representing the result
   */
  public boolean isLocal(final long dataId) {
    return localPartitionId == partitionFunc.partition(dataId);
  }

}
