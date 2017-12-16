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
package edu.snu.spl.cruise.ps.mlapps.lda;

import edu.snu.spl.cruise.ps.core.client.ETCruiseConfiguration;
import edu.snu.spl.cruise.ps.jobserver.client.JobLauncher;
import edu.snu.spl.cruise.ps.mlapps.lda.LDAParameters.Alpha;
import edu.snu.spl.cruise.ps.mlapps.lda.LDAParameters.Beta;
import edu.snu.spl.cruise.ps.mlapps.lda.LDAParameters.NumTopics;
import edu.snu.spl.cruise.ps.mlapps.lda.LDAParameters.NumVocabs;
import edu.snu.spl.cruise.utils.StreamingSerializableCodec;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;

/**
 * Run Latent Dirichlet Allocation algorithm on cruise JobServer.
 * Input dataset should be preprocessed to have continuous (no missing) vocabulary indices.
 */
public final class LDAJob {

  @Inject
  private LDAJob() {
  }

  public static void main(final String[] args) {
    JobLauncher.submitJob("LDA", args, ETCruiseConfiguration.newBuilder()
        .setTrainerClass(LDATrainer.class)
        .setInputParserClass(LDAETDataParser.class)
        .setInputKeyCodecClass(StreamingSerializableCodec.class)
        .setInputValueCodecClass(LDADataCodec.class)
        .setModelUpdateFunctionClass(LDAETModelUpdateFunction.class)
        .setModelKeyCodecClass(StreamingSerializableCodec.class)
        .setModelValueCodecClass(SparseArrayCodec.class)
        .setModelUpdateValueCodecClass(SerializableCodec.class)
        .addParameterClass(Alpha.class)
        .addParameterClass(Beta.class)
        .addParameterClass(NumTopics.class)
        .addParameterClass(NumVocabs.class)
        .build());
  }
}
