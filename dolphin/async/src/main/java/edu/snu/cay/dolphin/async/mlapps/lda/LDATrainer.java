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

import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.async.TrainingDataProvider;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.*;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Assign a random topic to each word in all documents and block on a global barrier to make sure
 * all workers update their initial topic assignments. For each iteration, sequentially sampling documents,
 * it immediately pushes the changed topic assignment whenever each word is sampled to a new topic.
 */
final class LDATrainer implements Trainer {

  private static final Logger LOG = Logger.getLogger(LDATrainer.class.getName());

  private final LDADataParser dataParser;
  private final LDABatchParameterWorker batchParameterWorker;
  private final SparseLDASampler sampler;
  private final LDAStatCalculator statCalculator;
  private final int numVocabs;
  private final List<Integer> vocabList;

  private final DataIdFactory<Long> idFactory;
  private final MemoryStore<Long> memoryStore;

  private final ParameterWorker<Integer, int[], int[]> parameterWorker;
  private final TrainingDataProvider<Long> trainingDataProvider;

  @Inject
  private LDATrainer(final LDADataParser dataParser,
                     final LDABatchParameterWorker batchParameterWorker,
                     final SparseLDASampler sampler,
                     final LDAStatCalculator statCalculator,
                     final DataIdFactory<Long> idFactory,
                     final MemoryStore<Long> memoryStore,
                     final ParameterWorker<Integer, int[], int[]> parameterWorker,
                     final TrainingDataProvider<Long> trainingDataProvider,
                     @Parameter(NumVocabs.class)final int numVocabs) {
    this.dataParser = dataParser;
    this.batchParameterWorker = batchParameterWorker;
    this.sampler = sampler;
    this.statCalculator = statCalculator;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
    this.parameterWorker = parameterWorker;
    this.trainingDataProvider = trainingDataProvider;
    this.numVocabs = numVocabs;

    // key numVocabs is a summary vector of word-topic distribution, in a form of numTopics-dimensional vector
    this.vocabList = new ArrayList<>(numVocabs + 1);
    for (int i = 0; i < numVocabs + 1; i++) {
      vocabList.add(i);
    }
  }

  @Override
  public void initialize() {
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
    }
    batchParameterWorker.pushAndClear();

    LOG.log(Level.INFO, "All random topic assignments are updated");
  }

  @Override
  public void run(final int iteration) {
    LOG.log(Level.INFO, "Iteration Started");

    int numSampledDocuments = 0;
    int miniBatchCount = 0;
    Map<Long, Document> workLoadMap = trainingDataProvider.getNextTrainingData();
    while (!workLoadMap.isEmpty()) {
      final List<Document> workLoad = new ArrayList<>(workLoadMap.values());
      final int batchSize = workLoad.size();

      sampler.sample(workLoad);

      numSampledDocuments += batchSize;
      miniBatchCount++;
      LOG.log(Level.INFO, "{0} documents have been sampled, mini-batch count is {1}",
          new Object[]{numSampledDocuments, miniBatchCount});

      workLoadMap = trainingDataProvider.getNextTrainingData();
    }

    LOG.log(Level.INFO, "Start computing log likelihood");
    final List<int[]> wordTopicCounts = parameterWorker.pull(vocabList);
    // numVocabs'th element of wordTopicCounts is a summary vector of word-topic distribution,
    // in a form of numTopics-dimensional vector
    final int[] wordTopicCountsSummary = wordTopicCounts.remove(numVocabs);
    final Map<Long, Document> totalWorkloadMap = memoryStore.getAll();
    final List<Document> totalWorkLoad = new ArrayList<>(totalWorkloadMap.values());
    LOG.log(Level.INFO, "App metric log: {0}", buildAppMetrics(statCalculator.computeDocLLH(totalWorkLoad),
        statCalculator.computeWordLLH(wordTopicCounts, wordTopicCountsSummary)));

    LOG.log(Level.INFO, "Iteration Ended");
  }

  @Override
  public void cleanup() {
  }

  private Metrics buildAppMetrics(final double docLLH, final double wordLLH) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.DOC_LLH, docLLH);
    appMetricMap.put(MetricKeys.WORD_LLH, wordLLH);

    return Metrics.newBuilder()
        .setData(appMetricMap)
        .build();
  }
}
