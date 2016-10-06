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

import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.async.TrainingDataProvider;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
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

  /**
   * Number of training data instances to be processed per mini-batch.
   */
  private final int miniBatchSize;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;
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
                     final TrainingDataProvider<Long> trainingDataProvider,
                     final MetricsMsgSender<WorkerMetrics> metricsMsgSender,
                     @Parameter(NumVocabs.class) final int numVocabs,
                     @Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize) {
    this.dataParser = dataParser;
    this.batchParameterWorker = batchParameterWorker;
    this.sampler = sampler;
    this.statCalculator = statCalculator;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
    this.parameterWorker = parameterWorker;
    this.trainingDataProvider = trainingDataProvider;
    this.numVocabs = numVocabs;
    this.miniBatchSize = miniBatchSize;

    // key numVocabs is a summary vector of word-topic distribution, in a form of numTopics-dimensional vector
    this.vocabList = new ArrayList<>(numVocabs + 1);
    for (int i = 0; i < numVocabs + 1; i++) {
      vocabList.add(i);
    }

    this.metricsMsgSender = metricsMsgSender;
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
    batchParameterWorker.pushAndClear(computeTracer, pushTracer);

    LOG.log(Level.INFO, "Number of instances per mini-batch = {0}", miniBatchSize);
    LOG.log(Level.INFO, "All random topic assignments are updated");
  }

  @Override
  public void run(final int iteration) {
    final long epochStartTime = System.currentTimeMillis();

    // Record the number of EM data blocks at the beginning of this iteration
    // to filter out stale metrics for optimization
    final int numEMBlocks = memoryStore.getNumBlocks();

    int miniBatchIdx = 0;
    int numDocumentsSampled = 0;
    final List<Document> totalDocumentsSampled = new LinkedList<>();

    Map<Long, Document> nextTrainingData = trainingDataProvider.getNextTrainingData();
    Collection<Document> documents = nextTrainingData.values();
    int numInstancesToProcess = documents.size();
    while (!nextTrainingData.isEmpty()) {
      resetTracers();
      final long miniBatchStartTime = System.currentTimeMillis();

      sampler.sample(documents, computeTracer, pushTracer, pullTracer);

      // update the documents processed so far
      numDocumentsSampled += numInstancesToProcess;
      totalDocumentsSampled.addAll(documents);
      LOG.log(Level.INFO, "{0} documents have been sampled until mini-batch {1}",
          new Object[]{numDocumentsSampled, miniBatchIdx});

      // load the set of training data instances to process in the next mini-batch
      nextTrainingData = trainingDataProvider.getNextTrainingData();

      final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;
      final WorkerMetrics miniBatchMetric =
          buildMiniBatchMetric(iteration, miniBatchIdx, numInstancesToProcess, miniBatchElapsedTime);
      LOG.log(Level.INFO, "WorkerMetrics {0}", miniBatchMetric);
      sendMetrics(miniBatchMetric);

      documents = nextTrainingData.values();
      numInstancesToProcess = documents.size();
      miniBatchIdx++;
    }

    LOG.log(Level.INFO, "Pull model to compute log likelihood");
    final List<int[]> wordTopicCounts = parameterWorker.pull(vocabList);
    // numVocabs'th element of wordTopicCounts is a summary vector of word-topic distribution,
    // in a form of numTopics-dimensional vector
    final int[] wordTopicCountsSummary = wordTopicCounts.remove(numVocabs);

    LOG.log(Level.INFO, "Start computing log likelihood");
    final double docLLH = statCalculator.computeDocLLH(totalDocumentsSampled);
    final double wordLLH = statCalculator.computeWordLLH(wordTopicCounts, wordTopicCountsSummary);
    final double epochElapsedTime = (System.currentTimeMillis() - epochStartTime) / 1000.0D;

    final WorkerMetrics epochMetric =
        buildEpochMetric(iteration, miniBatchIdx, numEMBlocks, numDocumentsSampled, docLLH, wordLLH, epochElapsedTime);
    LOG.log(Level.INFO, "WorkerMetrics {0}", epochMetric);
    sendMetrics(epochMetric);
  }

  @Override
  public void cleanup() {
  }

  private void resetTracers() {
    computeTracer.resetTrace();
    pushTracer.resetTrace();
    pullTracer.resetTrace();
  }

  private void sendMetrics(final WorkerMetrics workerMetrics) {
    LOG.log(Level.FINE, "Sending WorkerMetrics {0}", workerMetrics);

    metricsMsgSender.send(workerMetrics);
  }

  private WorkerMetrics buildMiniBatchMetric(final int iteration, final int miniBatchIdx,
                                             final int numProcessedDataItemCount, final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.DVT, numProcessedDataItemCount / elapsedTime);

    return WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder()
            .setData(appMetricMap)
            .build())
        .setEpochIdx(iteration)
        .setMiniBatchSize(miniBatchSize)
        .setMiniBatchIdx(miniBatchIdx)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .setTotalCompTime(computeTracer.totalElapsedTime())
        .setTotalPullTime(pullTracer.totalElapsedTime())
        .setAvgPullTime(pullTracer.avgTimePerElem())
        .setTotalPushTime(pushTracer.totalElapsedTime())
        .setAvgPushTime(pushTracer.avgTimePerElem())
        .setParameterWorkerMetrics(parameterWorker.buildParameterWorkerMetrics())
        .build();
  }

  private WorkerMetrics buildEpochMetric(final int iteration, final int numMiniBatchForEpoch,
                                         final int numDataBlocks, final int numProcessedDataItemCount,
                                         final double docLLH, final double wordLLH,
                                         final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.DOC_LLH, docLLH);
    appMetricMap.put(MetricKeys.WORD_LLH, wordLLH);
    parameterWorker.buildParameterWorkerMetrics(); // clear ParameterWorker metrics

    return WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder()
            .setData(appMetricMap)
            .build())
        .setEpochIdx(iteration)
        .setMiniBatchSize(miniBatchSize)
        .setNumMiniBatchForEpoch(numMiniBatchForEpoch)
        .setNumDataBlocks(numDataBlocks)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .build();
  }
}
