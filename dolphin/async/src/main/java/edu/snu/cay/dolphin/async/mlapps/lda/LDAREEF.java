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

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.*;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;

/**
 * Run Latent Dirichlet Allocation algorithm on dolphin-async.
 * Input dataset should be preprocessed to have continuous (no missing) vocabulary indices.
 */
public final class LDAREEF {

  @Inject
  private LDAREEF() {
  }

  public static void main(final String[] args) {
    AsyncDolphinLauncher.launch("LDAREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setTrainerClass(LDATrainer.class)
        .setUpdaterClass(LDAUpdater.class)
        .setParserClass(LDADataParser.class)
        .setTestDataParserClass(LDAETDataParser.class)
        .setKeyCodecClass(SerializableCodec.class)
        .setPreValueCodecClass(SerializableCodec.class)
        .setValueCodecClass(SparseArrayCodec.class)
        .setWorkerSerializerClass(LDADataSerializer.class)
        .addParameterClass(Alpha.class)
        .addParameterClass(Beta.class)
        .addParameterClass(NumTopics.class)
        .addParameterClass(NumVocabs.class)
        .build());
  }

}
