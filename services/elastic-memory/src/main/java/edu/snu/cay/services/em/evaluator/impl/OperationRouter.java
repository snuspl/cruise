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
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * OperationRouter that redirects incoming operations on specific data ids to corresponding evaluators.
 */
public final class OperationRouter {

  private static final Logger LOG = Logger.getLogger(OperationRouter.class.getName());

  private String localEndPointId;

  private String evalPrefix;

  private final int localPartitionId;

  private final PartitionFunc partitionFunc;

  @Inject
  private OperationRouter(final PartitionFunc partitionFunc,
                          @Parameter(PartitionId.class) final int partitionId) {
    this.localPartitionId = partitionId;
    this.partitionFunc = partitionFunc;

  }

  /**
   * Initialize the router.
   */
  public void initialize(final String endPointId) {
    this.localEndPointId = endPointId;
    this.evalPrefix = endPointId.split("-")[0];
    LOG.info("localEndPointId: " + localEndPointId);
  }

  /**
   * Return an endpoint id of evaluator that owns a data whose key is {@code dataId}.
   *
   * @param dataId Identifier of data
   * @return A pair of a boolean representing locality of data and an endpoint id of an evaluator
   */
  public Pair<Boolean, String> route(final long dataId) {

    final int partitionId = (int) partitionFunc.partition(dataId);
    if (localPartitionId == partitionId) {
      return new Pair<>(true, localEndPointId);
    } else {
      return new Pair<>(false, evalPrefix + '-' + partitionId);
    }
  }
}
