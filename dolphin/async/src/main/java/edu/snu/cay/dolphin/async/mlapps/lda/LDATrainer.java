/*
 * Copyright (C) 2017 Seoul National University
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
import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Assign a random topic to each word in all documents and block on a global barrier to make sure
 * all workers update their initial topic assignments. For each mini-batch, sequentially sampling documents,
 * it immediately pushes the changed topic assignment whenever each word is sampled to a new topic.
 */
final class LDATrainer implements Trainer<Document> {

  private static final Logger LOG = Logger.getLogger(LDATrainer.class.getName());

  private final SparseLDASampler sampler;
  private final LDAStatCalculator statCalculator;
  private final int numVocabs;
  private final int numTopics;

  private final List<Integer> vocabList;

  private final TrainingDataProvider<Long, Document> trainingDataProvider;

  private final ModelAccessor<Integer, int[], int[]> modelAccessor;

  /**
   * Allows to access and update the latest model.
   */
  private final ModelHolder<LDAModel> modelHolder;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final Tracer computeTracer;

  @Inject
  private LDATrainer(final SparseLDASampler sampler,
                     final LDAStatCalculator statCalculator,
                     final TrainingDataProvider<Long, Document> trainingDataProvider,
                     final ModelAccessor<Integer, int[], int[]> modelAccessor,
                     final ModelHolder<LDAModel> modelHolder,
                     @Parameter(NumVocabs.class) final int numVocabs,
                     @Parameter(NumTopics.class) final int numTopics,
                     @Parameter(DolphinParameters.MiniBatchSize.class) final int miniBatchSize,
                     @Parameter(DolphinParameters.NumTrainerThreads.class) final int numTrainerThreads) {
    this.sampler = sampler;
    this.statCalculator = statCalculator;
    this.trainingDataProvider = trainingDataProvider;
    this.modelAccessor = modelAccessor;
    this.numVocabs = numVocabs;

    // key numVocabs is a summary vector of word-topic distribution, in a form of numTopics-dimensional vector
    this.vocabList = new ArrayList<>(numVocabs + 1);
    for (int i = 0; i < numVocabs + 1; i++) {
      vocabList.add(i);
    }
    this.numTopics = numTopics;

    this.modelHolder = modelHolder;
    this.computeTracer = new Tracer();

    LOG.log(Level.INFO, "Number of Trainer threads = {0}", numTrainerThreads);
    LOG.log(Level.INFO, "Number of instances per mini-batch = {0}", miniBatchSize);
    LOG.log(Level.INFO, "All random topic assignments are updated");
  }

  @Override
  public void initGlobalSettings() {
    // In LDA, topic counts should be initialized by pushing values before running.
    final TopicChanges topicChanges = new TopicChanges();
    final Map<Long, Document> data = trainingDataProvider.getEpochData();
    for (final Document document : data.values()) {
      for (int i = 0; i < document.size(); i++) {
        final int word = document.getWord(i);
        topicChanges.increment(word, document.getAssignment(i), 1);
        // numVocabs-th row represents the total word-topic assignment count vector
        topicChanges.increment(numVocabs, document.getAssignment(i), 1);
      }
    }
    pushAndResetGradients(topicChanges);
    
    // TODO #487: Metric collecting should be done by the system, not manually by the user code.
    // The main purpose of this invocation is to reset ModelAccessor's tracers for the next round.
    modelAccessor.getAndResetMetrics();
  }

  @Override
  public MiniBatchResult runMiniBatch(final Collection<Document> miniBatchTrainingData) {
    resetTracers();

    final long miniBatchStartTime = System.currentTimeMillis();

    final List<Integer> words = getKeys(miniBatchTrainingData);
    final int numInstancesToProcess = miniBatchTrainingData.size();

    pullModels(words);

    computeTracer.startTimer();
    final List<TopicChanges> results = sampler.sample(miniBatchTrainingData);
    computeTracer.recordTime(numInstancesToProcess);

    final TopicChanges aggregated = aggregateChanges(results);

    // push gradients
    pushAndResetGradients(aggregated);

    final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;

    return buildMiniBatchResult(numInstancesToProcess, miniBatchElapsedTime);
  }

  @Override
  public EpochResult onEpochFinished(final Collection<Document> epochTrainingData,
                                     final Collection<Document> testData,
                                     final int epochIdx) {

    LOG.log(Level.INFO, "Pull model to compute log likelihood");
    final List<int[]> wordTopicCounts = modelAccessor.pull(vocabList);
    final int[] wordTopicCountsSummary = wordTopicCounts.remove(numVocabs);

    LOG.log(Level.INFO, "Start computing log likelihood");
    final double docLLH = statCalculator.computeDocLLH(epochTrainingData);
    final double wordLLH = statCalculator.computeWordLLH(wordTopicCounts, wordTopicCountsSummary);

    return buildEpochResult(docLLH, wordLLH);
  }

  private void pullModels(final List<Integer> words) {
    final List<int[]> topicVectors = modelAccessor.pull(words);

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

    modelHolder.resetModel(new LDAModel(topicSummaryVector, wordTopicVectors));
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

      modelAccessor.push(changedWord, parameters);
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
  }

  private MiniBatchResult buildMiniBatchResult(final int numProcessedDataItemCount, final double elapsedTime) {
    // TODO #487: Metric collecting should be done by the system, not manually by the user code.
    final Map<String, Double> modelAccessorMetrics = modelAccessor.getAndResetMetrics();
    return MiniBatchResult.newBuilder()
        .setAppMetric(MetricKeys.DVT, numProcessedDataItemCount / elapsedTime)
        .setComputeTime(computeTracer.totalElapsedTime())
        .setTotalPullTime(modelAccessorMetrics.get(ModelAccessor.METRIC_TOTAL_PULL_TIME_SEC))
        .setTotalPushTime(modelAccessorMetrics.get(ModelAccessor.METRIC_TOTAL_PUSH_TIME_SEC))
        .setAvgPullTime(modelAccessorMetrics.get(ModelAccessor.METRIC_AVG_PULL_TIME_SEC))
        .setAvgPushTime(modelAccessorMetrics.get(ModelAccessor.METRIC_AVG_PUSH_TIME_SEC))
        .build();
  }

  private EpochResult buildEpochResult(final double docLLH, final double wordLLH) {
    // TODO #487: Metric collecting should be done by the system, not manually by the user code.
    // The main purpose of this invocation is to reset ModelAccessor's tracers for the next round.
    modelAccessor.getAndResetMetrics();
    
    return EpochResult.newBuilder()
        .addAppMetric(MetricKeys.DOC_LLH, docLLH)
        .addAppMetric(MetricKeys.WORD_LLH, wordLLH)
        .build();
  }
}
