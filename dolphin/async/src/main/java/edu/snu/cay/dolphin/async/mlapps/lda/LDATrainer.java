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

import com.google.common.collect.Table;
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.async.EpochInfo;
import edu.snu.cay.dolphin.async.MiniBatchInfo;
import edu.snu.cay.dolphin.async.ModelAccessor;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.*;
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
final class LDATrainer implements Trainer<Document> {

  private static final Logger LOG = Logger.getLogger(LDATrainer.class.getName());

  private final SparseLDASampler sampler;
  private final LDAStatCalculator statCalculator;
  private final int numVocabs;
  private final int numTopics;

  private final List<Integer> vocabList;

  private final MemoryStore<Long> memoryStore;

  private final ParameterWorker<Integer, int[], int[]> parameterWorker;


  /**
   * Number of Trainer threads that train concurrently.
   */
  private final int numTrainerThreads;

  /**
   * Allows to access and update the latest model.
   */
  private final ModelAccessor<LDAModel> modelAccessor;

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
  private LDATrainer(final SparseLDASampler sampler,
                     final LDAStatCalculator statCalculator,
                     final MemoryStore<Long> memoryStore,
                     final ParameterWorker<Integer, int[], int[]> parameterWorker,
                     final MetricsMsgSender<WorkerMetrics> metricsMsgSender,
                     final ModelAccessor<LDAModel> modelAccessor,
                     @Parameter(NumVocabs.class) final int numVocabs,
                     @Parameter(NumTopics.class) final int numTopics,
                     @Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                     @Parameter(Parameters.NumTrainerThreads.class) final int numTrainerThreads) {
    this.sampler = sampler;
    this.statCalculator = statCalculator;
    this.memoryStore = memoryStore;
    this.parameterWorker = parameterWorker;
    this.numVocabs = numVocabs;
    this.miniBatchSize = miniBatchSize;
    this.numTrainerThreads = numTrainerThreads;

    // key numVocabs is a summary vector of word-topic distribution, in a form of numTopics-dimensional vector
    this.vocabList = new ArrayList<>(numVocabs + 1);
    for (int i = 0; i < numVocabs + 1; i++) {
      vocabList.add(i);
    }
    this.numTopics = numTopics;

    this.modelAccessor = modelAccessor;
    this.metricsMsgSender = metricsMsgSender;
    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  @Override
  public void initialize() {
    // In LDA, topic counts should be initialized by pushing values before running.
    final TopicChanges topicChanges = new TopicChanges();
    final Map<Long, Document> data = memoryStore.getAll();
    for (final Document document : data.values()) {
      for (int i = 0; i < document.size(); i++) {
        final int word = document.getWord(i);
        topicChanges.increment(word, document.getAssignment(i), 1);
        // numVocabs-th row represents the total word-topic assignment count vector
        topicChanges.increment(numVocabs, document.getAssignment(i), 1);
      }
    }
    pushAndResetGradients(topicChanges);

    LOG.log(Level.INFO, "Number of Trainer threads = {0}", numTrainerThreads);
    LOG.log(Level.INFO, "Number of instances per mini-batch = {0}", miniBatchSize);
    LOG.log(Level.INFO, "All random topic assignments are updated");
  }

  @Override
  public void runMiniBatch(final Collection<Document> miniBatchData, final MiniBatchInfo miniBatchInfo) {
    final int epochIdx = miniBatchInfo.getEpochIdx();
    final int miniBatchIdx = miniBatchInfo.getMiniBatchIdx();
    final long miniBatchStartTime = System.currentTimeMillis();

    final List<Integer> words = getKeys(miniBatchData);
    final int numInstancesToProcess = miniBatchData.size();

    pullModels(words);

    computeTracer.startTimer();
    final List<TopicChanges> results = sampler.sample(miniBatchData);
    computeTracer.recordTime(numInstancesToProcess);

    final TopicChanges aggregated = aggregateChanges(results);

    // push gradients
    pushAndResetGradients(aggregated);

    final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;
    final WorkerMetrics miniBatchMetric =
        buildMiniBatchMetric(epochIdx, miniBatchIdx, numInstancesToProcess, miniBatchElapsedTime);
    LOG.log(Level.INFO, "WorkerMetrics {0}", miniBatchMetric);
    sendMetrics(miniBatchMetric);
  }

  @Override
  public void onEpochFinished(final Collection<Document> epochData,
                              final EpochInfo epochInfo) {

    LOG.log(Level.INFO, "Pull model to compute log likelihood");
    final List<int[]> wordTopicCounts = parameterWorker.pull(vocabList);
    final int[] wordTopicCountsSummary = wordTopicCounts.remove(numVocabs);

    LOG.log(Level.INFO, "Start computing log likelihood");
    final double docLLH = statCalculator.computeDocLLH(epochData);
    final double wordLLH = statCalculator.computeWordLLH(wordTopicCounts, wordTopicCountsSummary);


    final int epochIdx = epochInfo.getEpochIdx();
    final int numMiniBatches = epochInfo.getNumMiniBatches();
    final int numEMBlocks = epochInfo.getNumEMBlocks();
    final double epochElapsedTime = (System.currentTimeMillis() - epochInfo.getEpochStartTime()) / 1000.0D;

    final WorkerMetrics epochMetric =
        buildEpochMetric(epochIdx, numMiniBatches, numEMBlocks, epochData.size(), docLLH, wordLLH, epochElapsedTime);
    LOG.log(Level.INFO, "WorkerMetrics {0}", epochMetric);
    sendMetrics(epochMetric);
  }

  private void pullModels(final List<Integer> words) {
    pullTracer.startTimer();
    final List<int[]> topicVectors = parameterWorker.pull(words);
    pullTracer.recordTime(words.size());

    final int[] sparseTopicSummaryVector = topicVectors.remove(words.size() - 1);
    // i-th element of topicSummaryVector represents total number of assignments of i-th topic
    final int[] topicSummaryVector = new int[numTopics];
    for (int i = 0; i < sparseTopicSummaryVector.length; i++) {
      final int topic = sparseTopicSummaryVector[i++];
      final int count = sparseTopicSummaryVector[i];
      topicSummaryVector[topic] = count;
    }

    final Map<Integer, int[]> wordTopicVectors = new HashMap<>(topicVectors.size());
    for (int i = 0; i < topicVectors.size(); ++i) {
      wordTopicVectors.put(words.get(i), topicVectors.get(i));
    }

    modelAccessor.resetModel(new LDAModel(topicSummaryVector, wordTopicVectors));
  }

  /**
   * Aggregates the changed topics computed by each thread.
   * @param results a list of the number of changes per topic.
   * @return Sum of the number of changes per topic.
   */
  private TopicChanges aggregateChanges(final List<TopicChanges> results) {
    final TopicChanges aggregated = new TopicChanges();
    results.forEach(
        result -> {
          final Table<Integer, Integer, Integer> changedTopicCounts = result.getTable();
          changedTopicCounts.cellSet().forEach(
              cell -> {
                if (cell.getValue() != 0) {
                  aggregated.increment(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
                }
              }
          );
        });
    return aggregated;
  }

  private void pushAndResetGradients(final TopicChanges topicChanges) {
    final Table<Integer, Integer, Integer> changedTopicCount = topicChanges.getTable();
    for (final int changedWord : changedTopicCount.rowKeySet()) {
      computeTracer.startTimer();
      final Map<Integer, Integer> changedTopicCountsForWord = changedTopicCount.row(changedWord);
      final int numChangedTopics = changedTopicCountsForWord.size();

      // Given a word, an even index represents a changed topic index and a corresponding odd index represents
      // a changed value for the topic index.
      final int[] parameters = new int[2 * numChangedTopics];
      int i = 0;
      for (final Map.Entry<Integer, Integer> entry : changedTopicCountsForWord.entrySet()) {
        parameters[2 * i] = entry.getKey();
        parameters[2 * i + 1] = entry.getValue();
        i++;
      }
      computeTracer.recordTime(0);

      pushTracer.startTimer();
      parameterWorker.push(changedWord, parameters);
      pushTracer.recordTime(1);
    }
    changedTopicCount.clear();
  }

  @Override
  public void cleanup() {
  }

  private List<Integer> getKeys(final Collection<Document> documents) {
    computeTracer.startTimer();

    final Set<Integer> keys = new TreeSet<>();
    for (final Document document : documents) {
      keys.addAll(document.getWords());
    }

    final List<Integer> result = new ArrayList<>(keys.size() + 1);
    result.addAll(keys);
    // numVocabs-th row represents the total word-topic assignment count vector
    result.add(numVocabs);

    computeTracer.recordTime(0);

    return result;
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
