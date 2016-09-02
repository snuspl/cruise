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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.async.TrainingDataParser;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MLRTrainingDataParser implements TrainingDataParser {
  private static final Logger LOG = Logger.getLogger(MLRTrainingDataParser.class.getName());

  /**
   * Number of instances to compute training loss with.
   */
  private final int trainErrorDatasetSize;

  /**
   * Parser object for fetching and parsing the input dataset.
   */
  private final MLRParser mlrParser;

  private final DataIdFactory<Long> idFactory;
  private final MemoryStore<Long> memoryStore;

  @Inject
  MLRTrainingDataParser(@Parameter(MLRParameters.TrainErrorDatasetSize.class) final int trainErrorDatasetSize,
                        final MLRParser mlrParser,
                        final DataIdFactory<Long> idFactory,
                        final MemoryStore<Long> memoryStore) {
    this.trainErrorDatasetSize = trainErrorDatasetSize;
    this.mlrParser = mlrParser;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
  }

  @Override
  public void parseData() {
    // The input dataset, given as a list of pairs which are in the form, (input vector, label).
    final List<Pair<Vector, Integer>> dataValues = mlrParser.parse();

    final List<Long> dataKeys;
    try {
      dataKeys = idFactory.getIds(dataValues.size());
    } catch (final IdGenerationException e) {
      throw new RuntimeException("Fail to generate data keys", e);
    }

    memoryStore.putList(dataKeys, dataValues);

    LOG.log(Level.INFO, "Total number of training data items = {0}", dataValues.size());
    if (dataValues.size() < trainErrorDatasetSize) {
      LOG.log(Level.WARNING, "Number of samples is less than trainErrorDatasetSize = {0}", trainErrorDatasetSize);
    }
  }
}
