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
package edu.snu.spl.cruise.services.et.examples.checkpoint;

import edu.snu.spl.cruise.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.spl.cruise.services.et.evaluator.api.Table;
import edu.snu.spl.cruise.services.et.evaluator.api.TableAccessor;
import edu.snu.spl.cruise.services.et.evaluator.api.Tasklet;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Task code that puts values to the table.
 */
final class PutTask implements Tasklet {
  private static final Logger LOG = Logger.getLogger(PutTask.class.getName());
  static final String VALUE_PREFIX = "valueForKey";
  static final int NUM_ITEMS = 100;

  private final String executorId;

  private final TableAccessor tableAccessor;

  @Inject
  private PutTask(@Parameter(ExecutorIdentifier.class) final String executorId,
                  final TableAccessor tableAccessor) {
    this.executorId = executorId;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public void run() throws Exception {
    LOG.log(Level.INFO, "Hello! I am {0}", new Object[]{executorId});
    final Table<Long, String, ?> table = tableAccessor.getTable(CheckpointETDriver.TABLE_ID);

    for (long key = 0; key < NUM_ITEMS; key++) {
      table.put(key, VALUE_PREFIX + key).get();
    }

    LOG.log(Level.INFO, "Succeed to put {0} key-value items", NUM_ITEMS);
  }

  @Override
  public void close() {

  }
}
