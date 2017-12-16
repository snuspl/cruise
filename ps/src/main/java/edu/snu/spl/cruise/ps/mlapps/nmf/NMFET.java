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

import edu.snu.spl.cruise.ps.core.client.ETCruiseConfiguration;
import edu.snu.spl.cruise.ps.core.client.ETCruiseLauncher;
import edu.snu.spl.cruise.ps.mlapps.serialization.DenseVectorCodec;
import edu.snu.spl.cruise.utils.IntegerCodec;
import edu.snu.spl.cruise.utils.LongCodec;

import static edu.snu.spl.cruise.ps.mlapps.nmf.NMFParameters.*;

/**
 * Client for non-negative matrix factorization via SGD.
 */
public final class NMFET {

  /**
   * Should not be instantiated.
   */
  private NMFET() {
  }

  public static void main(final String[] args) {
    ETCruiseLauncher.launch("NMFET", args, ETCruiseConfiguration.newBuilder()
        .setTrainerClass(NMFTrainer.class)
        .setInputParserClass(NMFETDataParser.class)
        .setInputKeyCodecClass(LongCodec.class)
        .setInputValueCodecClass(NMFDataCodec.class)
        .setModelUpdateFunctionClass(NMFETModelUpdateFunction.class)
        .setModelKeyCodecClass(IntegerCodec.class)
        .setModelValueCodecClass(DenseVectorCodec.class)
        .setModelUpdateValueCodecClass(DenseVectorCodec.class)
        .addParameterClass(Rank.class)
        .addParameterClass(PrintMatrices.class)
        .build());
  }
}
