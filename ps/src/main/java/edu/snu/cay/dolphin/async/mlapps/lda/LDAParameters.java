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

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used for latent Dirichlet allocation.
 */
final class LDAParameters {

  @NamedParameter(doc = "a parameter of the Dirichlet prior on the per-document topic distribution",
      short_name = "alpha", default_value = "0.001")
  static final class Alpha implements Name<Double> {
  }

  @NamedParameter(doc = "a parameter of the Dirichlet prior on the per-topic word distribution",
      short_name = "beta", default_value = "0.01")
  static final class Beta implements Name<Double> {
  }

  @NamedParameter(doc = "number of topics", short_name = "num_topics", default_value = "100")
  static final class NumTopics implements Name<Integer> {
  }

  @NamedParameter(doc = "number of unique words in the corpus, which is equal to (largest word index + 1)",
      short_name = "num_vocabs")
  static final class NumVocabs implements Name<Integer> {
  }

  static final class MetricKeys {
    // The key denoting a portion of log likelihood (P(z)) computed from document-topic distribution.
    static final String DOC_LLH =
        "LDA_TRAINER_DOC_LLH";

    // The key denoting a portion of log likelihood (P(w|z)) computed from word-topic distribution.
    static final String WORD_LLH =
        "LDA_TRAINER_WORD_LLH";

  }
}
