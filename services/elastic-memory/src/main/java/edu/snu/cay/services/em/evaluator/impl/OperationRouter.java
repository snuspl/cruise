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
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OperationRouter that redirects incoming operations on specific data ids to corresponding evaluators.
 */
@Private
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
    LOG.log(Level.INFO, "Initialize router with localEndPointId: {0}", localEndPointId);
  }

  /**
   * Returns the routing result for the given {@code dataId}.
   * It returns the endpoint id of the evaluator that owns the data whose id is {@code dataId}.
   * A boolean value is piggybacked, which indicates whether the data is in the local memory store.
   * So the caller does not need to check that the target evaluator is local or not.
   *
   * @param dataId an id of data
   * @return a pair of a boolean representing locality of data and an endpoint id of a target evaluator
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
