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

import edu.snu.cay.async.AsyncDolphinConfiguration;
import edu.snu.cay.async.AsyncDolphinLauncher;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import javax.inject.Inject;

/**
 *
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

  @NamedParameter(short_name = "alpha", default_value = "0.001")
  public static final class Alpha implements Name<Double> {
  }

  @NamedParameter(short_name = "beta", default_value = "0.01")
  public static final class Beta implements Name<Double> {
  }

  @NamedParameter(short_name = "num_topics", default_value = "100")
  public static final class NumTopics implements Name<Integer> {
  }

  @NamedParameter(short_name = "num_vocabs")
  public static final class NumVocabs implements Name<Integer> {
  }
}
