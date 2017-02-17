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

import static edu.snu.cay.services.et.examples.simple.SimpleETDriver.TABLE0_ID;
import static edu.snu.cay.services.et.examples.simple.SimpleETDriver.TABLE1_ID;

/**
 * Task code for subscribers for simple example.
 */
final class SubscriberTask implements Task {
  private static final Logger LOG = Logger.getLogger(SubscriberTask.class.getName());
  static final long KEY0 = 0;
  static final long KEY1 = 1;
  static final String VALUE0 = "Table0";
  static final String VALUE1 = "Table1";

  private final String elasticTableId;

  private final String executorId;

  private final TableAccessor tableAccessor;

  @Inject
  private SubscriberTask(@Parameter(ETIdentifier.class) final String elasticTableId,
                         @Parameter(ExecutorIdentifier.class) final String executorId,
                         final TableAccessor tableAccessor) {
    this.elasticTableId = elasticTableId;
    this.executorId = executorId;
    this.tableAccessor = tableAccessor;
  }

  @Override
  @SuppressWarnings("unchecked")
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello, {0}! I am a subscriber with executor id {1}",
        new Object[]{elasticTableId, executorId});
    final Table<Long, String> table0 = tableAccessor.get(TABLE0_ID);
    final Table<Long, String> table1 = tableAccessor.get(TABLE1_ID);

    table0.put(KEY0, VALUE0);
    LOG.log(Level.INFO, "Put value {0} to key {1} in table0", new Object[]{VALUE0, KEY0});
    table1.put(KEY1, VALUE1);
    LOG.log(Level.INFO, "Put value {0} to key {1} in table1", new Object[]{VALUE1, KEY1});

    // wait to be in line with associator tasks that are waiting for a subscriber task to put values to tables
    Thread.sleep(5000);

    final String value00 = table0.get(KEY0);
    final String value01 = table0.get(KEY1);
    final String value10 = table1.get(KEY0);
    final String value11 = table1.get(KEY1);

    LOG.log(Level.INFO, "value for key {0} in a table0 is {1}", new Object[]{KEY0, value00});
    LOG.log(Level.INFO, "value for key {0} in a table0 is {1}", new Object[]{KEY1, value01});
    LOG.log(Level.INFO, "value for key {0} in a table1 is {1}", new Object[]{KEY0, value10});
    LOG.log(Level.INFO, "value for key {0} in a table1 is {1}", new Object[]{KEY1, value11});

    if (!value00.equals(VALUE0) || value01 != null ||
        !value11.equals(VALUE1) || value10 != null) {
      throw new RuntimeException("The result is different from the expectation");
    }

    // wait until all executors get values from tables
    Thread.sleep(5000);
    return null;
  }
}
