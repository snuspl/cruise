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
package edu.snu.cay.services.et.examples.simple;

import edu.snu.cay.services.et.configuration.parameters.ETIdentifier;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.simple.SimpleETDriver.HASHED_TABLE_ID;
import static edu.snu.cay.services.et.examples.simple.SimpleETDriver.ORDERED_TABLE_ID;
import static edu.snu.cay.services.et.examples.simple.PutTask.*;

/**
 * Task code that gets values from tables.
 * It should be submitted after {@link PutTask} is done.
 */
final class GetTask implements Task {
  private static final Logger LOG = Logger.getLogger(GetTask.class.getName());

  private final String elasticTableId;

  private final String executorId;

  private final TableAccessor tableAccessor;

  @Inject
  private GetTask(@Parameter(ETIdentifier.class) final String elasticTableId,
                  @Parameter(ExecutorIdentifier.class) final String executorId,
                  final TableAccessor tableAccessor) {
    this.elasticTableId = elasticTableId;
    this.executorId = executorId;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello, {0}! I am an executor id {1}", new Object[]{elasticTableId, executorId});
    final Table<Long, String, ?> hashedTable = tableAccessor.getTable(HASHED_TABLE_ID);
    final Table<Long, String, ?> orderedTable = tableAccessor.getTable(ORDERED_TABLE_ID);

    final String value00 = hashedTable.getOrInit(KEY0).get();
    final String value01 = hashedTable.getOrInit(KEY1).get();
    final String value10 = orderedTable.getOrInit(KEY0).get();
    final String value11 = orderedTable.getOrInit(KEY1).get();

    LOG.log(Level.INFO, "value for key {0} in a hashedTable is {1}", new Object[]{KEY0, value00});
    LOG.log(Level.INFO, "value for key {0} in a hashedTable is {1}", new Object[]{KEY1, value01});
    LOG.log(Level.INFO, "value for key {0} in an orderedTable is {1}", new Object[]{KEY0, value10});
    LOG.log(Level.INFO, "value for key {0} in an orderedTable is {1}", new Object[]{KEY1, value11});

    if (!value00.equals(VALUE0) || !value01.equals(SimpleUpdateFunction.getInitValue(KEY1)) ||
        !value11.equals(VALUE1) || !value10.equals(SimpleUpdateFunction.getInitValue(KEY0))) {
      throw new RuntimeException("The result is different from the expectation");
    }

    return null;
  }
}
