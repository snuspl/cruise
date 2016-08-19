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

import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.*;
import org.apache.commons.math3.special.Gamma;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;

/**
 * Compute log likelihoods of the model.
 * This follows T. L. Griffiths and M. Steyvers. Finding scientific topics. Proceedings of the National
 * Academy of Sciences of the United States of America, Vol. 101, No. Suppl 1. (6 April 2004), pp. 5228-5235.
 */
final class LDAStats {

  private final double alpha;
  private final double beta;
  private final int numTopics;
  private final int numVocabs;

  private final double logGammaAlpha;
  private final double logGammaBeta;

  @Inject
  private LDAStats(@Parameter(Alpha.class) final double alpha,
                   @Parameter(Beta.class) final double beta,
                   @Parameter(NumTopics.class) final int numTopics,
                   @Parameter(LDAParameters.NumVocabs.class) final int numVocabs) {
    this.alpha = alpha;
    this.beta = beta;
    this.numTopics = numTopics;
    this.numVocabs = numVocabs;

    this.logGammaAlpha = Gamma.logGamma(alpha);
    this.logGammaBeta = Gamma.logGamma(beta);
  }

  double computeDocLLH(final Collection<Document> workload) {
    double result = workload.size() * (Gamma.logGamma(numTopics * alpha) - numTopics * Gamma.logGamma(alpha));
    for (final Document doc : workload) {
      for (int topic = 0; topic < numTopics; topic++) {
        final int topicCount = doc.getTopicCount(topic);
        result += topicCount == 0 ? logGammaAlpha : Gamma.logGamma(topicCount + alpha);
      }
      result -= Gamma.logGamma(doc.size() + numTopics * alpha);
    }
    return result;
  }

  double computeWordLLH(final Collection<int[]> wordTopicCounts, final int[] wordTopicCountsSummary) {
    double result = numTopics * (Gamma.logGamma(numVocabs * beta) - numVocabs * Gamma.logGamma(beta));
    for (final int[] wordTopicCount : wordTopicCounts) {
      for (int i = 0; i < numTopics; i++) {
        result += wordTopicCount[i] == 0 ? logGammaBeta : Gamma.logGamma(wordTopicCount[i] + beta);
      }
    }
    for (int i = 0; i < numTopics; i++) {
      result -= Gamma.logGamma(wordTopicCountsSummary[i] + numVocabs * beta);
    }
    return result;
  }
}
