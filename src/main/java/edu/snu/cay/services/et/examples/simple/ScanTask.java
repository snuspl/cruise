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
import edu.snu.cay.services.et.evaluator.api.Tablet;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.simple.SimpleETDriver.ORDERED_TABLE_WITH_FILE_ID;

/**
 * Task code that scans values in locally assigned table blocks.
 * This task assumes that there's no data migration during its span.
 */
final class ScanTask implements Task {
  private static final Logger LOG = Logger.getLogger(ScanTask.class.getName());

  private final String elasticTableId;

  private final String executorId;

  private final TableAccessor tableAccessor;

  @Inject
  private ScanTask(@Parameter(ETIdentifier.class) final String elasticTableId,
                   @Parameter(ExecutorIdentifier.class) final String executorId,
                   final TableAccessor tableAccessor) {
    this.elasticTableId = elasticTableId;
    this.executorId = executorId;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello, {0}! I am an executor id {1}", new Object[]{elasticTableId, executorId});
    final Table<Long, String, ?> orderedTableWithFile = tableAccessor.getTable(ORDERED_TABLE_WITH_FILE_ID);

    final Tablet<Long, String, ?> localTablet = orderedTableWithFile.getLocalTablet();

    final Map<Long, String> localDataMap = localTablet.getDataMap();
    final Iterator<Entry<Long, String>> localDataIter = localTablet.getDataIterator();

    // validate that data obtained with both ways are same
    // it's true only when there's no background data migration
    final AtomicInteger dataCount = new AtomicInteger(0);
    localDataIter.forEachRemaining(localDataKVPair -> {
      if (!localDataMap.containsKey(localDataKVPair.getKey())) {
        throw new RuntimeException("Data obtained through getLocalDataMap and getLocalDataIterator should be same.");
      }
      LOG.log(Level.INFO, "{0}th data. key: {1}, value: {2}",
          new Object[]{dataCount.incrementAndGet(), localDataKVPair.getKey(), localDataKVPair.getValue()});
    });

    if (localDataMap.size() != dataCount.get()) {
      throw new RuntimeException("Data obtained through getLocalDataMap and getLocalDataIterator should be same.");
    }

    return null;
  }
}
