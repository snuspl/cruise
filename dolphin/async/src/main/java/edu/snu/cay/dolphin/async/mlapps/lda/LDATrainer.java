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
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.metric.Tracer;
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
  private final int numMiniBatchPerIter;

  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  @Inject
  private LDATrainer(final LDADataParser dataParser,
                     final LDABatchParameterWorker batchParameterWorker,
                     final SparseLDASampler sampler,
                     final LDAStatCalculator statCalculator,
                     final DataIdFactory<Long> idFactory,
                     final MemoryStore<Long> memoryStore,
                     final ParameterWorker<Integer, int[], int[]> parameterWorker,
                     @Parameter(NumVocabs.class) final int numVocabs,
                     @Parameter(Parameters.MiniBatches.class) final int numMiniBatchPerIter) {
    this.dataParser = dataParser;
    this.batchParameterWorker = batchParameterWorker;
    this.sampler = sampler;
    this.statCalculator = statCalculator;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
    this.parameterWorker = parameterWorker;
    this.numVocabs = numVocabs;
    this.numMiniBatchPerIter = numMiniBatchPerIter;

    // key numVocabs is a summary vector of word-topic distribution, in a form of numTopics-dimensional vector
    this.vocabList = new ArrayList<>(numVocabs + 1);
    for (int i = 0; i < numVocabs + 1; i++) {
      vocabList.add(i);
    }

    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();
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

    final Map<Long, Document> workloadMap = memoryStore.getAll();
    final List<Document> workload = new ArrayList<>(workloadMap.values());
    final int numDocuments = workload.size();
    int numSampledDocuments = 0;

    for (int batchIdx = 0; batchIdx < numMiniBatchPerIter; batchIdx++) {
      final int batchSize = numDocuments / numMiniBatchPerIter
          + ((numDocuments % numMiniBatchPerIter > batchIdx) ? 1 : 0);

      sampler.sample(workload.subList(numSampledDocuments, numSampledDocuments + batchSize),
          computeTracer, pushTracer, pullTracer);

      numSampledDocuments += batchSize;
      LOG.log(Level.INFO, "{0} documents out of {1} have been sampled",
          new Object[]{numSampledDocuments, numDocuments});
    }

    LOG.log(Level.INFO, "Start computing log likelihood");
    final List<int[]> wordTopicCounts = parameterWorker.pull(vocabList);
    // numVocabs'th element of wordTopicCounts is a summary vector of word-topic distribution,
    // in a form of numTopics-dimensional vector
    final int[] wordTopicCountsSummary = wordTopicCounts.remove(numVocabs);
    LOG.log(Level.INFO, "App metric log: {0}", buildAppMetrics(statCalculator.computeDocLLH(workload),
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
