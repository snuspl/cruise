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
package edu.snu.cay.services.et.examples.load;

import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Load task which checks which data is stored in which table.
 */
public final class LoadETTask implements Task {

  private static final Logger LOG = Logger.getLogger(LoadETTask.class.getName());
  private String taskId;
  private final boolean isKeyValueTable;
  private final TableAccessor tableAccessor;

  @Inject
  private LoadETTask(@Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
                     @Parameter(LoadETDriver.IsKeyValueTable.class) final boolean isKeyValueTable,
                     final TableAccessor tableAccessor) {
    this.taskId = taskId;
    this.isKeyValueTable = isKeyValueTable;
    this.tableAccessor = tableAccessor;
  }

  /**
   * Log All keys and values which are stored in local tablet.
   */
  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    final Table<Long, String, ?> table = isKeyValueTable ? tableAccessor.getTable(LoadETDriver.KEY_VALUE_TABLE) :
        tableAccessor.getTable(LoadETDriver.NONE_KEY_TABLE);
    table.getLocalTablet().getDataMap().forEach((key, value) -> {
      LOG.log(Level.INFO, "TaskId : {0}, table Id : {1}, key : {2}, value : {3}",
          new Object[]{taskId, LoadETDriver.KEY_VALUE_TABLE, key, value});
    });
    return null;
  }
}
