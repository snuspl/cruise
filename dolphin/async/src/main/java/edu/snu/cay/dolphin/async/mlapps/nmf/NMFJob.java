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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import edu.snu.cay.dolphin.async.ETDolphinConfiguration;
import edu.snu.cay.dolphin.async.jobserver.NMFJobLauncher;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import org.apache.reef.io.serialization.SerializableCodec;

import static edu.snu.cay.dolphin.async.mlapps.nmf.NMFParameters.*;

/**
 * Client for non-negative matrix factorization via SGD with JobServer.
 */
public final class NMFJob {

  /**
   * Should not be instantiated.
   */
  private NMFJob() {
  }

  public static void main(final String[] args) {
    NMFJobLauncher.launch("MatrixFactorizationJob", args, ETDolphinConfiguration.newBuilder()
        .setTrainerClass(NMFTrainer.class)
        .setInputParserClass(NMFETDataParser.class)
        .setInputKeyCodecClass(SerializableCodec.class)
        .setInputValueCodecClass(NMFDataCodec.class)
        .setModelUpdateFunctionClass(NMFETModelUpdateFunction.class)
        .setModelKeyCodecClass(SerializableCodec.class)
        .setModelValueCodecClass(DenseVectorCodec.class)
        .setModelUpdateValueCodecClass(DenseVectorCodec.class)
        .addParameterClass(Rank.class)
        .addParameterClass(StepSize.class)
        .addParameterClass(Lambda.class)
        .addParameterClass(PrintMatrices.class)
        .addParameterClass(DecayPeriod.class)
        .addParameterClass(DecayRate.class)
        .build());
  }
}
