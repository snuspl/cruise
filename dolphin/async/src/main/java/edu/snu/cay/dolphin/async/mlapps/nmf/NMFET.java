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

import edu.snu.cay.dolphin.async.client.ETDolphinConfiguration;
import edu.snu.cay.dolphin.async.client.ETDolphinLauncher;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.utils.IntegerCodec;
import edu.snu.cay.utils.LongCodec;

import static edu.snu.cay.dolphin.async.mlapps.nmf.NMFParameters.*;

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
    ETDolphinLauncher.launch("NMFET", args, ETDolphinConfiguration.newBuilder()
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
