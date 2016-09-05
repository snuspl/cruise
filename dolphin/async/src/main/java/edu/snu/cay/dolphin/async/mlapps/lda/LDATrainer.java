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
import edu.snu.cay.dolphin.async.TrainingDataDivider;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.*;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
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

  private final SparseLDASampler sampler;
  private final LDAStatCalculator statCalculator;
  private final int numVocabs;
  private final List<Integer> vocabList;

  private final MemoryStore<Long> memoryStore;

  private final TrainingDataDivider<Long> trainingDataDivider;

  private final ParameterWorker<Integer, int[], int[]> parameterWorker;

  @Inject
  private LDATrainer(final SparseLDASampler sampler,
                     final LDAStatCalculator statCalculator,
                     final MemoryStore<Long> memoryStore,
                     final TrainingDataDivider<Long> trainingDataDivider,
                     final ParameterWorker<Integer, int[], int[]> parameterWorker,
                     @Parameter(NumVocabs.class) final int numVocabs) {
    this.sampler = sampler;
    this.statCalculator = statCalculator;
    this.memoryStore = memoryStore;
    this.trainingDataDivider = trainingDataDivider;
    this.parameterWorker = parameterWorker;
    this.numVocabs = numVocabs;
    // key numVocabs is a summary vector of word-topic distribution, in a form of numTopics-dimensional vector
    this.vocabList = new ArrayList<>(numVocabs + 1);
    for (int i = 0; i < numVocabs + 1; i++) {
      vocabList.add(i);
    }
  }

  @Override
  public void initialize() {

  }

  @Override
  public void onEpochStart(final int epoch) {
    LOG.log(Level.INFO, "Epoch Started");
  }

  @Override
  public void onEpochEnd(final int epoch) {
    LOG.log(Level.INFO, "Start computing log likelihood");
    final List<int[]> wordTopicCounts = parameterWorker.pull(vocabList);
    // numVocabs'th element of wordTopicCounts is a summary vector of word-topic distribution,
    // in a form of numTopics-dimensional vector
    final int[] wordTopicCountsSummary = wordTopicCounts.remove(numVocabs);
    final Map<Long, Document> workloadMap = memoryStore.getAll();
    final Collection<Document> workload = workloadMap.values();
    LOG.log(Level.INFO, "App metric log: {0}", buildAppMetrics(statCalculator.computeDocLLH(workload),
        statCalculator.computeWordLLH(wordTopicCounts, wordTopicCountsSummary)));

    LOG.log(Level.INFO, "Epoch Ended");
  }

  @Override
  public void run(final int miniBatch) {

    Map<Long, Document> workloadMap = trainingDataDivider.getNextTrainingDataSplit();
    while (!workloadMap.isEmpty()) {
      final Collection<Document> workload = workloadMap.values();

      final int numDocuments = workload.size();
      final int countForLogging = numDocuments / 3;
      int numSampledDocuments = 0;

      for (final Document document : workload) {
        sampler.sample(document);
        numSampledDocuments++;

        if (numSampledDocuments % countForLogging == 0) {
          LOG.log(Level.INFO, "{0}/{1} documents are sampled", new Object[]{numSampledDocuments, numDocuments});
        }
      }

      workloadMap = trainingDataDivider.getNextTrainingDataSplit();
    }
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
