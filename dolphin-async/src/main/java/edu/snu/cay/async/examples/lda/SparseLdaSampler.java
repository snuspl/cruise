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
package edu.snu.cay.async.examples.lda;

import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

final class SparseLdaSampler {

  private final double alpha;
  private final double beta;
  private final int numTopics;
  private final int numVocabs;
  private final ParameterWorker<Integer, int[], int[]> parameterWorker;

  @Inject
  private SparseLdaSampler(@Parameter(LdaREEF.Alpha.class) final double alpha,
                           @Parameter(LdaREEF.Beta.class) final double beta,
                           @Parameter(LdaREEF.NumTopics.class) final int numTopics,
                           @Parameter(LdaREEF.NumVocabs.class) final int numVocabs,
                           final ParameterWorker<Integer, int[], int[]> parameterWorker) {
    this.alpha = alpha;
    this.beta = beta;
    this.numTopics = numTopics;
    this.numVocabs = numVocabs;
    this.parameterWorker = parameterWorker;
  }

  void sample(final Document document) {
    final int[] globalWordCountByTopics = parameterWorker.pull(numVocabs);
    double sumS = 0.0;
    double sumR = 0.0;
    double sumQ = 0.0;
    final double[] sTerms = new double[numTopics];
    final double[] rTerms = new double[numTopics];
    final double[] qTerms = new double[numTopics];

    final double[] qCoefficients = new double[numTopics];

    // Initialize auxiliary variables
    for (int i = 0; i < numTopics; i++) {
      final int topicCount = document.getTopicCount(i);
      final double denom = globalWordCountByTopics[i] + beta * numVocabs;
      qCoefficients[i] = (alpha + topicCount) / denom;
      sTerms[i] = alpha * beta / denom;
      sumS += sTerms[i];

      if (topicCount != 0) {
        rTerms[i] = (topicCount * beta) / denom;
        sumR += rTerms[i];
      }
    }

    for (int itr = 0; itr < document.size(); itr++) {
      final int word = document.getWord(itr);
      final int oldTopic = document.getAssignment(itr);
      final int oldTopicCount = document.getTopicCount(oldTopic);

      // Remove the current word from the document and update terms.
      final double denom = globalWordCountByTopics[oldTopic] + beta * numVocabs;
      sumS -= sTerms[oldTopic];
      sTerms[oldTopic] = (alpha * beta) / (denom - 1);
      sumS += sTerms[oldTopic];

      sumR -= rTerms[oldTopic];
      rTerms[oldTopic] = ((oldTopicCount - 1) * beta) / (denom - 1);
      sumR += rTerms[oldTopic];

      qCoefficients[oldTopic] = (alpha + oldTopicCount - 1) / (denom - 1);

      document.removeWord(itr);

      final int[] wordTopicCount = parameterWorker.pull(word);

      // Calculate q terms
      sumQ = 0.0;

      for (int i = 0; i < numTopics; i++) {
        qTerms[i] = 0.0;
        final int count = wordTopicCount[i];
        if (count != 0) {
          qTerms[i] = qCoefficients[i] * count;
          sumQ += qTerms[i];
        }
      }

      // Sample a new topic based on the terms
      final double randomVar = Math.random() * (sumS + sumR + sumQ);
      final int newTopic;

      // Hit the "smoothing only" bucket.
      if (randomVar < sumS) {
        newTopic = sampleFromTerms(randomVar - (sumR + sumQ), sTerms);
      } else if (sumS <= randomVar && randomVar < sumS + sumR) { // Hit the "document topic" bucket
        newTopic = sampleFromTerms(randomVar - (sumQ + sumS), rTerms);
      } else { // Hit the "topic word" bucket. More than 90% hit here.
        newTopic = sampleFromTerms(randomVar - (sumS + sumR), qTerms);
      }

      final int newTopicCount = document.getTopicCount(newTopic);

      // Update the terms and add the removed word with the new topic.
      final double newDenom = globalWordCountByTopics[newTopic] + beta * numVocabs;
      sumS -= sTerms[newTopic];
      sTerms[newTopic] = (alpha * beta) / (newDenom + 1);
      sumS += sTerms[newTopic];

      sumR -= rTerms[newTopic];
      rTerms[newTopic] = ((newTopicCount + 1) * beta) / (newDenom + 1);
      sumR += rTerms[newTopic];

      qCoefficients[newTopic] = (alpha + newTopicCount + 1) / (newDenom + 1);

      document.addWord(itr, newTopic);

      // Push the changes to the parameter servers.
      parameterWorker.push(word, new int[]{oldTopic, -1});
      parameterWorker.push(word, new int[]{newTopic, 1});
      parameterWorker.push(numVocabs, new int[]{oldTopic, -1});
      parameterWorker.push(numVocabs, new int[]{newTopic, 1});
    }
  }

  private int sampleFromTerms(final double randomVar, final double[] terms) {
    double val = randomVar;
    for (int i = 0; i < numTopics; i++) {
      if (val < terms[i]) {
        return i;
      }

      val -= terms[i];
    }

    throw new RuntimeException("Unexpected situation with the uniform distribution");
  }
}
