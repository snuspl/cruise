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
package edu.snu.cay.dolphin.examples.ml.algorithms.classification;

import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.core.WorkloadPartition;
import edu.snu.cay.dolphin.examples.ml.data.Row;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.commons.lang.math.LongRange;

import javax.inject.Inject;
import java.util.*;

public final class LogisticRegPreCmpTask extends UserComputeTask {

  private final DataParser<List<Row>> dataParser;
  private final WorkloadPartition workloadPartition;
  private final MemoryStore memoryStore;
  private final DataIdFactory<Long> dataIdFactory;

  @Inject
  private LogisticRegPreCmpTask(final DataParser<List<Row>> dataParser,
                                final WorkloadPartition workloadPartition,
                                final MemoryStore memoryStore,
                                final DataIdFactory<Long> dataIdFactory) {
    this.dataParser = dataParser;
    this.workloadPartition = workloadPartition;
    this.memoryStore = memoryStore;
    this.dataIdFactory = dataIdFactory;
  }

  @Override
  public void initialize() throws ParseException {
    final List<Row> rows = dataParser.get();
    try {
      final List<Long> ids = dataIdFactory.getIds(rows.size());

      // Below code assume that dataIdFactory returns consecutive ids
      final Set<LongRange> workload = new HashSet<>();
      final Set<LongRange> rangeSet = new HashSet<>();
      rangeSet.add(new LongRange(ids.get(0).longValue(), ids.size() - 1));
      workload.addAll(rangeSet);

      workloadPartition.initialize(workload);
      memoryStore.putList(ids, rows);
    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run(final int iteration) {

  }
}
