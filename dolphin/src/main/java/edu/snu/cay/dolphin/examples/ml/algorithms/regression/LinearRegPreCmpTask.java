/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.examples.ml.algorithms.regression;

import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.examples.ml.data.RowDataType;
import edu.snu.cay.dolphin.examples.ml.data.Row;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

public final class LinearRegPreCmpTask extends UserComputeTask {

  /**
   * Key used in Elastic Memory to put/get the data.
   */
  private final String dataType;

  private final DataParser<List<Row>> dataParser;
  private final MemoryStore memoryStore;
  private final DataIdFactory<Long> dataIdFactory;

  @Inject
  private LinearRegPreCmpTask(@Parameter(RowDataType.class) final String dataType,
                              final DataParser<List<Row>> dataParser,
                              final MemoryStore memoryStore,
                              final DataIdFactory<Long> dataIdFactory) {
    this.dataType = dataType;
    this.dataParser = dataParser;
    this.memoryStore = memoryStore;
    this.dataIdFactory = dataIdFactory;
  }

  @Override
  public void initialize() throws ParseException {
    final List<Row> rows = dataParser.get();
    try {
      final List<Long> ids = dataIdFactory.getIds(rows.size());
      memoryStore.putList(dataType, ids, rows);
    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run(final int iteration) {

  }
}
