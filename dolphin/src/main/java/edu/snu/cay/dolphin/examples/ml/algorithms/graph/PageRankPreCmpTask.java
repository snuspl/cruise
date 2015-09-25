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
package edu.snu.cay.dolphin.examples.ml.algorithms.graph;

import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

public final class PageRankPreCmpTask extends UserComputeTask {

  /**
   * Key used in Elastic Memory to put/get the data.
   * TODO #168: we should find better place to put this
   */
  public static final String KEY_GRAPH = "graph";

  /**
   * Adjacency list data parser.
   */
  private final DataParser<Map<Integer, List<Integer>>> dataParser;

  /**
   * Memory storage to put/get the data.
   */
  private final MemoryStore memoryStore;

  @Inject
  private PageRankPreCmpTask(final DataParser<Map<Integer, List<Integer>>> dataParser,
                             final MemoryStore memoryStore) {
    this.dataParser = dataParser;
    this.memoryStore = memoryStore;
  }

  @Override
  public void initialize() throws ParseException {
    final Map<Integer, List<Integer>> subgraphs = dataParser.get();
    for (final Map.Entry<Integer, List<Integer>> entry : subgraphs.entrySet()) {
      memoryStore.getElasticStore().put(KEY_GRAPH, (long) entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void run(final int iteration) {

  }
}
