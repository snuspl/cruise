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
package edu.snu.cay.services.et.examples.checkpoint;

import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Task code that gets values from the table to confirm that the table has values put by {@link PutTask}.
 */
final class GetTask implements Task {
  private static final Logger LOG = Logger.getLogger(GetTask.class.getName());

  private final String executorId;

  private final TableAccessor tableAccessor;

  @Inject
  private GetTask(@Parameter(ExecutorIdentifier.class) final String executorId,
                  final TableAccessor tableAccessor) {
    this.executorId = executorId;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello! I am {1}", new Object[]{executorId});
    final Table<Long, String, ?> table = tableAccessor.getTable(CheckpointETDriver.TABLE_ID);

    final String value0 = table.get(0L).get();
    if (value0 == null || !value0.equals(PutTask.VALUE0)) {
      throw new RuntimeException();
    }

    final String value1 = table.get(1L).get();
    if (value1 == null || !value1.equals(PutTask.VALUE1)) {
      throw new RuntimeException();
    }

    LOG.log(Level.INFO, "Succeed to get all values");

    return null;
  }
}
