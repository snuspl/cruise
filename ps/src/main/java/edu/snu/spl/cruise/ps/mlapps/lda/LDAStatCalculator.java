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
package edu.snu.spl.cruise.ps.mlapps.lda;

import edu.snu.spl.cruise.ps.mlapps.lda.LDAParameters.*;
import org.apache.commons.math3.special.Gamma;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;

/**
 * Compute log likelihoods of the model.
 * This follows T. L. Griffiths and M. Steyvers. Finding scientific topics. Proceedings of the National
 * Academy of Sciences of the United States of America, Vol. 101, No. Suppl 1. (6 April 2004), pp. 5228-5235.
 */
final class LDAStatCalculator {

  private final double alpha;
  private final double beta;
  private final int numTopics;
  private final int numVocabs;

  /**
   * Pre-computed constant to save the cost for computing a frequent term, log(Gamma(alpha)).
   */
  private final double logGammaAlpha;

  /**
   * Pre-computed constant to save the cost for computing a frequent term, log(Gamma(beta)).
   */
  private final double logGammaBeta;

  @Inject
  private LDAStatCalculator(@Parameter(Alpha.class) final double alpha,
                            @Parameter(Beta.class) final double beta,
                            @Parameter(NumTopics.class) final int numTopics,
                            @Parameter(NumVocabs.class) final int numVocabs) {
    this.alpha = alpha;
    this.beta = beta;
    this.numTopics = numTopics;
    this.numVocabs = numVocabs;

    this.logGammaAlpha = Gamma.logGamma(alpha);
    this.logGammaBeta = Gamma.logGamma(beta);
  }

  /**
   * Computes log likelihood for documents (P(z)) according to Eq. [3] in the reference.
   * <ul>
   *   <li>T: {@code numTopics}</li>
   *   <li>D: Total number of documents</li>
   *   <li>n(j, d): <i>j</i>th topic's number of assignments to <i>d</i>th document</li>
   * </ul>
   * @param workload a collection of documents assigned to this trainer
   * @return a portion of log likelihood computed from the given workload
   */
  double computeDocLLH(final Collection<Document> workload) {
    double result = workload.size() * (Gamma.logGamma(numTopics * alpha) - numTopics * Gamma.logGamma(alpha));
    for (final Document doc : workload) {
      for (int j = 0; j < numTopics; j++) {
        final int topicCount = doc.getTopicCount(j);
        if (topicCount < 0) {
          doc.setTopicCount(j, 0);
        }
        result += topicCount <= 0 ? logGammaAlpha : Gamma.logGamma(topicCount + alpha);
      }
      result -= Gamma.logGamma(doc.size() + numTopics * alpha);
    }
    return result;
  }

  /**
   * Computes log likelihood for word-topic vectors (P(w|z)) according to Eq. [2] in the reference.
   * <ul>
   *   <li>T: {@code numTopics}</li>
   *   <li>W: {@code numVocabs}</li>
   *   <li>n(j, w): <i>j</i>th topic's number of assignments to <i>w</i>th vocabulary</li>
   * </ul>

   * @return a portion of log likelihood computed from the given word-topic vectors
   */
  double computeWordLLH(final Collection<int[]> wordTopicCounts, final int[] wordTopicCountsSummary) {
    double result = numTopics * (Gamma.logGamma(numVocabs * beta) - numVocabs * Gamma.logGamma(beta));
    for (final int[] wordTopicCount : wordTopicCounts) {
      // For computing log-likelihood, we need only the values. Please refer to SparseArrayCodec.
      for (int j = 1; j < wordTopicCount.length; j += 2) {
        result += Gamma.logGamma(wordTopicCount[j] + beta);
      }
      // handle the case of zero values separately
      result += logGammaBeta * (numTopics - wordTopicCount.length / 2);
    }
    for (int j = 1; j < wordTopicCountsSummary.length; j += 2) {
      result -= Gamma.logGamma(wordTopicCountsSummary[j] + numVocabs * beta);
    }
    // handle the case of zero values separately
    result -= Gamma.logGamma(numVocabs * beta) * (numTopics - wordTopicCountsSummary.length / 2);
    return result;
  }
}
