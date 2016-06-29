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

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import javax.inject.Inject;

/**
 * Run Latent Dirichlet Allocation algorithm on dolphin-async.
 */
public final class LdaREEF {

  @Inject
  private LdaREEF() {
  }

  public static void main(final String[] args) {
    AsyncDolphinLauncher.launch("LdaREEF", args, AsyncDolphinConfiguration.newBuilder()
        .addParameterClass(Alpha.class)
        .addParameterClass(Beta.class)
        .addParameterClass(NumTopics.class)
        .addParameterClass(NumVocabs.class)
        .setKeyCodecClass(SerializableCodec.class)
        .setPreValueCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdaterClass(LdaUpdater.class)
        .setWorkerClass(LdaWorker.class)
        .build());
  }

  /**
   * A parameter of the Dirichlet prior on the per-document topic distribution.
   */
  @NamedParameter(short_name = "alpha", default_value = "0.001")
  public static final class Alpha implements Name<Double> {
  }

  /**
   * A parameter of the Dirichlet prior on the per-topic word distribution.
   */
  @NamedParameter(short_name = "beta", default_value = "0.01")
  public static final class Beta implements Name<Double> {
  }

  /**
   * The number of topics.
   */
  @NamedParameter(short_name = "num_topics", default_value = "100")
  public static final class NumTopics implements Name<Integer> {
  }

  /**
   * The number of unique words in the corpus.
   */
  @NamedParameter(short_name = "num_vocabs")
  public static final class NumVocabs implements Name<Integer> {
  }
}
