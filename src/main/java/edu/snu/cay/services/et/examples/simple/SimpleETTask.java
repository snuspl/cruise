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
package edu.snu.cay.services.et.examples.simple;

import edu.snu.cay.services.et.configuration.parameters.ETIdentifier;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.evaluator.impl.LocalKeyGenerator;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.simple.SimpleETDriver.TABLE_ID;

/**
 * Task code for simple example.
 */
final class SimpleETTask implements Task {
  private static final Logger LOG = Logger.getLogger(SimpleETTask.class.getName());

  private final String elasticTableId;

  private final TableAccessor tableAccessor;

  @Inject
  private SimpleETTask(@Parameter(ETIdentifier.class) final String elasticTableId,
                       final TableAccessor tableAccessor) {
    this.elasticTableId = elasticTableId;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello, {0}!", elasticTableId);
    final Table<Long, String> table = tableAccessor.get(TABLE_ID);

    // TODO #27: Need to provide a way to access locally assigned keys
    LOG.log(Level.INFO, "value in a table: {0}", table.get(LocalKeyGenerator.SAMPLE_KEY));
    return null;
  }
}
