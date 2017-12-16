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
package edu.snu.spl.cruise.ps.mlapps.nmf;

import edu.snu.spl.cruise.ps.core.client.CruisePSConfiguration;
import edu.snu.spl.cruise.ps.jobserver.client.JobLauncher;
import edu.snu.spl.cruise.ps.mlapps.serialization.DenseVectorCodec;
import edu.snu.spl.cruise.utils.StreamingSerializableCodec;

import static edu.snu.spl.cruise.ps.mlapps.nmf.NMFParameters.*;

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
    JobLauncher.submitJob("NMF", args, CruisePSConfiguration.newBuilder()
        .setTrainerClass(NMFTrainer.class)
        .setInputParserClass(NMFDataParser.class)
        .setInputKeyCodecClass(StreamingSerializableCodec.class)
        .setInputValueCodecClass(NMFDataCodec.class)
        .setModelUpdateFunctionClass(NMFModelUpdateFunction.class)
        .setModelKeyCodecClass(StreamingSerializableCodec.class)
        .setModelValueCodecClass(DenseVectorCodec.class)
        .setModelUpdateValueCodecClass(DenseVectorCodec.class)
        .addParameterClass(Rank.class)
        .addParameterClass(PrintMatrices.class)
        .build());
  }
}
