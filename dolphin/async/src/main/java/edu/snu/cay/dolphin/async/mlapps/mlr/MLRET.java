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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.dolphin.async.ETDolphinConfiguration;
import edu.snu.cay.dolphin.async.ETDolphinLauncher;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import org.apache.reef.io.serialization.SerializableCodec;

import static edu.snu.cay.dolphin.async.mlapps.mlr.MLRParameters.*;

/**
 * Client for non-negative matrix factorization via SGD.
 */
public final class MLRET {

  /**
   * Should not be instantiated.
   */
  private MLRET() {
  }

  public static void main(final String[] args) {
    ETDolphinLauncher.launch("MLRET", args, ETDolphinConfiguration.newBuilder()
        .setTrainerClass(MLRTrainer.class)
        .setInputParserClass(MLRETDataParser.class)
        .setInputKeyCodecClass(SerializableCodec.class)
        .setInputValueCodecClass(MLRDataCodec.class)
        .setModelUpdateFunctionClass(MLRETModelUpdateFunction.class)
        .setModelKeyCodecClass(SerializableCodec.class)
        .setModelValueCodecClass(DenseVectorCodec.class)
        .setModelUpdateValueCodecClass(DenseVectorCodec.class)
        .addParameterClass(NumClasses.class)
        .addParameterClass(NumFeatures.class)
        .addParameterClass(InitialStepSize.class)
        .addParameterClass(Lambda.class)
        .addParameterClass(NumFeaturesPerPartition.class)
        .addParameterClass(ModelGaussian.class)
        .addParameterClass(DecayPeriod.class)
        .addParameterClass(DecayRate.class)
        .build());
  }
}
