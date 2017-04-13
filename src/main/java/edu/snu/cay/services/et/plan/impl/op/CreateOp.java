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
package edu.snu.cay.services.et.plan.impl.op;

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An operation to create a table.
 */
public final class CreateOp extends AbstractOp {
  private final Set<String> executorIds;
  private final TableConfiguration tableConf;

  public CreateOp(final Set<String> executorIds,
                  final TableConfiguration tableConf) {
    this.executorIds = executorIds;
    this.tableConf = tableConf;
  }

  @Override
  public ListenableFuture<?> execute(final ETMaster etMaster) throws PlanOpExecutionException {
    final List<AllocatedExecutor> executorList = new ArrayList<>(executorIds.size());

    for (final String executorId : executorIds) {
      try {
        executorList.add(etMaster.getExecutor(executorId));
      } catch (ExecutorNotExistException e) {
        throw new PlanOpExecutionException("Exception while executing " + toString(), e);
      }
    }

    return etMaster.createTable(tableConf, executorList);
  }

  @Override
  public String toString() {
    return "CreateOp{" +
        "executorIds=" + executorIds +
        ", tableConf=" + tableConf +
        '}';
  }
}
