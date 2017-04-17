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

import edu.snu.cay.services.et.common.util.concurrent.CompletedFuture;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;

import java.util.Map;

/**
 * An operation for deallocating the existing executor.
 */
public final class DeallocateOp extends AbstractOp {
  private final String executorId;

  public DeallocateOp(final String executorId) {
    this.executorId = executorId;
  }

  @Override
  public ListenableFuture<?> execute(final ETMaster etMaster, final Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException {
    try {
      etMaster.getExecutor(executorId).close();
    } catch (ExecutorNotExistException e) {
      throw new PlanOpExecutionException("Exception while executing " + toString(), e);
    }

    // TODO #96: add listener to close
    return new CompletedFuture<>(null);
  }

  @Override
  public String toString() {
    return "DeallocateOp{" +
        "executorId='" + executorId + '\'' +
        '}';
  }
}
