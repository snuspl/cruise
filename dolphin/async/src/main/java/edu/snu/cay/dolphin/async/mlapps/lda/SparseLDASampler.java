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

import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.*;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;

/**
 * Sample a batch of documents using the word-topic assignment count matrix from parameter server and a local
 * document-topic assignment count vector. It follows SparseLDA algorithm in L. Yao, D. Mimno, and A. McCallum.
 * Efficient methods for topic model inference on streaming document collections. In Proceedings of the
 * 15th ACM SIGKDD international conference on Knowledge discovery and data mining, pages 937–946. ACM, 2009.
 */
final class SparseLDASampler {

  private final double alpha;
  private final double beta;
  private final int numTopics;
  private final int numVocabs;
  private final ParameterWorker<Integer, int[], int[]> parameterWorker;
  private final LDABatchParameterWorker batchParameterWorker;

  @Inject
  private SparseLDASampler(@Parameter(Alpha.class) final double alpha,
                           @Parameter(Beta.class) final double beta,
                           @Parameter(NumTopics.class) final int numTopics,
                           @Parameter(NumVocabs.class) final int numVocabs,
                           final ParameterWorker<Integer, int[], int[]> parameterWorker,
                           final LDABatchParameterWorker batchParameterWorker) {
    this.alpha = alpha;
    this.beta = beta;
    this.numTopics = numTopics;
    this.numVocabs = numVocabs;
    this.parameterWorker = parameterWorker;
    this.batchParameterWorker = batchParameterWorker;
  }

  void sample(final Collection<Document> documents, final Tracer computeTracer,
              final Tracer pushTracer, final Tracer pullTracer) {
    computeTracer.startTimer();
    final List<Integer> words = getKeys(documents);
    computeTracer.recordTime(0);

    pullTracer.startTimer();
    final List<int[]> topicVectors = parameterWorker.pull(words);
    pullTracer.recordTime(words.size());

    computeTracer.startTimer();
    final int[] globalWordCountByTopics = topicVectors.remove(words.size() - 1);
    final int[] topicSummaryVector = new int[numTopics];
    for (int i = 0; i < globalWordCountByTopics.length; i++) {
      topicSummaryVector[globalWordCountByTopics[i++]] = globalWordCountByTopics[i];
    }

    final Map<Integer, int[]> wordTopicVectors = new HashMap<>(topicVectors.size());
    for (int i = 0; i < topicVectors.size(); ++i) {
      wordTopicVectors.put(words.get(i), topicVectors.get(i));
    }
    computeTracer.recordTime(0);

    for (final Document document : documents) {
      computeTracer.startTimer();
      double sumS = 0.0;
      double sumR = 0.0;
      double sumQ = 0.0;
      final double[] sTerms = new double[numTopics];
      final double[] rTerms = new double[numTopics];
      final List<Integer> nonZeroRTermIndices = new ArrayList<>(numTopics);
      final double[] qTerms = new double[numTopics];
      final List<Integer> nonZeroQTermIndices = new ArrayList<>(numTopics);

      final double[] qCoefficients = new double[numTopics];

      // Initialize auxiliary variables
      // Recalculate for each document to adapt changes from other workers.
      for (int i = 0; i < numTopics; i++) {
        final int topicCount = document.getTopicCount(i);
        final double denom = topicSummaryVector[i] + beta * numVocabs;
        qCoefficients[i] = (alpha + topicCount) / denom;
        // All s terms are not zero
        sTerms[i] = alpha * beta / denom;
        sumS += sTerms[i];

        if (topicCount != 0) {
          nonZeroRTermIndices.add(i);
          rTerms[i] = (topicCount * beta) / denom;
          sumR += rTerms[i];
        }
      }

      for (int wordIndex = 0; wordIndex < document.size(); wordIndex++) {
        final int word = document.getWord(wordIndex);
        final int oldTopic = document.getAssignment(wordIndex);
        final int oldTopicCount = document.getTopicCount(oldTopic);

        // Remove the current word from the document and update terms.
        final double denom = (topicSummaryVector[oldTopic] - 1) + beta * numVocabs;
        sumS -= sTerms[oldTopic];
        sTerms[oldTopic] = (alpha * beta) / denom;
        sumS += sTerms[oldTopic];

        sumR -= rTerms[oldTopic];
        rTerms[oldTopic] = ((oldTopicCount - 1) * beta) / denom;
        sumR += rTerms[oldTopic];

        // Remove from nonzero r terms if it goes to 0
        if (oldTopicCount == 1) {
          // Explicitly convert to Integer type not to call remove(int position)
          nonZeroRTermIndices.remove((Integer) oldTopic);
        }

        qCoefficients[oldTopic] = (alpha + oldTopicCount - 1) / denom;

        document.removeWordAtIndex(wordIndex);

        final int[] wordTopicCount = wordTopicVectors.get(word);

        // Calculate q terms
        nonZeroQTermIndices.clear();
        sumQ = 0.0;

        for (int i = 0; i < wordTopicCount.length; i++) {
          final int topic = wordTopicCount[i++];
          final int count = wordTopicCount[i];
          qTerms[topic] = qCoefficients[topic] * count;
          sumQ += qTerms[topic];
          nonZeroQTermIndices.add(topic);
        }

        // Sample a new topic based on the terms
        final double randomVar = Math.random() * (sumS + sumR + sumQ);
        final int newTopic;

        if (randomVar < sumS) {
          // Hit the "smoothing only" bucket.
          newTopic = sampleFromTerms(randomVar, sTerms);
        } else if (sumS <= randomVar && randomVar < sumS + sumR) {
          // Hit the "document topic" bucket.
          newTopic = sampleFromTerms(randomVar - sumS, rTerms, nonZeroRTermIndices);
        } else {
          // Hit the "topic word" bucket. More than 90% hit here.
          newTopic = sampleFromTerms(randomVar - (sumS + sumR), qTerms, nonZeroQTermIndices);
        }

        final int newTopicCount = document.getTopicCount(newTopic);

        // Update the terms and add the removed word with the new topic.
        final double newDenom = (topicSummaryVector[newTopic] + 1) + beta * numVocabs;
        sumS -= sTerms[newTopic];
        sTerms[newTopic] = (alpha * beta) / newDenom;
        sumS += sTerms[newTopic];

        sumR -= rTerms[newTopic];
        rTerms[newTopic] = ((newTopicCount + 1) * beta) / newDenom;
        sumR += rTerms[newTopic];

        // Add to nonzero r terms if it goes to 1
        if (newTopicCount == 0) {
          nonZeroRTermIndices.add(newTopic);
        }

        qCoefficients[newTopic] = (alpha + newTopicCount + 1) / newDenom;

        document.addWordAtIndex(wordIndex, newTopic);

        if (newTopic != oldTopic) {
          // Push the changes to the parameter servers
          batchParameterWorker.addTopicChange(word, oldTopic, -1);
          batchParameterWorker.addTopicChange(word, newTopic, 1);
          // numVocabs-th row represents the total word-topic assignment count vector
          batchParameterWorker.addTopicChange(numVocabs, oldTopic, -1);
          batchParameterWorker.addTopicChange(numVocabs, newTopic, 1);
        }
      }
      computeTracer.recordTime(1);
    }
    batchParameterWorker.pushAndClear(computeTracer, pushTracer);
  }

  private List<Integer> getKeys(final Collection<Document> documents) {
    final Set<Integer> keys = new TreeSet<>();
    for (final Document document : documents) {
      keys.addAll(document.getWords());
    }

    final List<Integer> result = new ArrayList<>(keys.size() + 1);
    result.addAll(keys);
    // numVocabs-th row represents the total word-topic assignment count vector
    result.add(numVocabs);

    return result;
  }

  private int sampleFromTerms(final double randomVar, final double[] terms) {
    double val = randomVar;
    for (int i = 0; i < terms.length; i++) {
      if (val < terms[i]) {
        return i;
      }

      val -= terms[i];
    }

    throw new RuntimeException("randomVar has to be smaller than summation of all terms");
  }

  private int sampleFromTerms(final double randomVar, final double[] terms, final List<Integer> nonzeroIndices) {
    double val = randomVar;
    for (final int nonzeroIndex : nonzeroIndices) {
      if (val < terms[nonzeroIndex]) {
        return nonzeroIndex;
      }

      val -= terms[nonzeroIndex];
    }

    throw new RuntimeException("randomVar has to be smaller than summation of all nonzero terms");
  }
}
