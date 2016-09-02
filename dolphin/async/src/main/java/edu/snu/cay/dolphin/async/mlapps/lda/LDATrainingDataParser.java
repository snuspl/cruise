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
package edu.snu.cay.dolphin.async.mlapps.lda;

import edu.snu.cay.dolphin.async.TrainingDataParser;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LDATrainingDataParser implements TrainingDataParser {
  private static final Logger LOG = Logger.getLogger(LDATrainingDataParser.class.getName());

  private final LDADataParser dataParser;
  private final DataIdFactory<Long> idFactory;
  private final MemoryStore<Long> memoryStore;
  private final LDABatchParameterWorker batchParameterWorker;

  private final int numVocabs;

  @Inject
  public LDATrainingDataParser(final LDADataParser dataParser,
                               final LDABatchParameterWorker batchParameterWorker,
                               final DataIdFactory<Long> idFactory,
                               final MemoryStore<Long> memoryStore,
                               @Parameter(LDAParameters.NumVocabs.class) final int numVocabs) {
    this.dataParser = dataParser;
    this.batchParameterWorker = batchParameterWorker;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
    this.numVocabs = numVocabs;
  }
  @Override
  public void parseData() {
    final List<Document> documents = dataParser.parse();
    final List<Long> dataKeys;

    try {
      dataKeys = idFactory.getIds(documents.size());
    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }

    memoryStore.putList(dataKeys, documents);

    for (final Document document : documents) {
      for (int i = 0; i < document.size(); i++) {
        final int word = document.getWord(i);
        batchParameterWorker.addTopicChange(word, document.getAssignment(i), 1);
        // numVocabs-th row represents the total word-topic assignment count vector
        batchParameterWorker.addTopicChange(numVocabs, document.getAssignment(i), 1);
      }
      batchParameterWorker.pushAndClear();
    }

    LOG.log(Level.INFO, "All random topic assignments are updated");
  }
}
