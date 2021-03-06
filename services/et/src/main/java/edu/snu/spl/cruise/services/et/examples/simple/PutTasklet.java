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
package edu.snu.spl.cruise.services.et.examples.simple;

import edu.snu.spl.cruise.services.et.configuration.parameters.ETIdentifier;
import edu.snu.spl.cruise.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.spl.cruise.services.et.evaluator.api.Table;
import edu.snu.spl.cruise.services.et.evaluator.api.TableAccessor;
import edu.snu.spl.cruise.services.et.evaluator.api.Tasklet;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.spl.cruise.services.et.examples.simple.SimpleETDriver.HASHED_TABLE_ID;
import static edu.snu.spl.cruise.services.et.examples.simple.SimpleETDriver.ORDERED_TABLE_ID;

/**
 * Task code that puts values to tables.
 */
final class PutTasklet implements Tasklet {
  private static final Logger LOG = Logger.getLogger(PutTasklet.class.getName());
  static final long KEY0 = 0;
  static final long KEY1 = 1;
  static final String VALUE0 = "Table0";
  static final String VALUE1 = "Table1";

  private final String elasticTableId;

  private final String executorId;

  private final TableAccessor tableAccessor;

  @Inject
  private PutTasklet(@Parameter(ETIdentifier.class) final String elasticTableId,
                     @Parameter(ExecutorIdentifier.class) final String executorId,
                     final TableAccessor tableAccessor) {
    this.elasticTableId = elasticTableId;
    this.executorId = executorId;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public void run() throws Exception {
    LOG.log(Level.INFO, "Hello, {0}! I am an executor id {1}", new Object[]{elasticTableId, executorId});
    final Table<Long, String, ?> hashedTable = tableAccessor.getTable(HASHED_TABLE_ID);
    final Table<Long, String, ?> orderedTable = tableAccessor.getTable(ORDERED_TABLE_ID);

    final String prevValue00 = hashedTable.put(KEY0, VALUE0).get();
    LOG.log(Level.INFO, "Put value {0} to key {1} in hashedTable", new Object[]{VALUE0, KEY0});
    final String prevValue11 = orderedTable.put(KEY1, VALUE1).get();
    LOG.log(Level.INFO, "Put value {0} to key {1} in orderedTable", new Object[]{VALUE1, KEY1});

    LOG.log(Level.INFO, "Prev value for key {0} in a hashedTable is {1}", new Object[]{KEY0, prevValue00});
    LOG.log(Level.INFO, "Prev value for key {0} in an orderedTable is {1}", new Object[]{KEY1, prevValue11});

    if (prevValue00 != null || prevValue11 != null) {
      throw new RuntimeException("The result is different from the expectation");
    }
  }

  @Override
  public void close() {

  }
}
