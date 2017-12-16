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
package edu.snu.spl.cruise.ps.mlapps.mlr;

import edu.snu.spl.cruise.ps.core.client.ETCruiseConfiguration;
import edu.snu.spl.cruise.ps.core.client.ETCruiseLauncher;
import edu.snu.spl.cruise.ps.mlapps.serialization.DenseVectorCodec;
import edu.snu.spl.cruise.utils.IntegerCodec;
import edu.snu.spl.cruise.utils.LongCodec;

import static edu.snu.spl.cruise.ps.mlapps.mlr.MLRParameters.*;

/**
 * Client for multinomial logistic regression via SGD.
 */
public final class MLRET {

  /**
   * Should not be instantiated.
   */
  private MLRET() {
  }

  public static void main(final String[] args) {
    ETCruiseLauncher.launch("MLRET", args, ETCruiseConfiguration.newBuilder()
        .setTrainerClass(MLRTrainer.class)
        .setInputParserClass(MLRETDataParser.class)
        .setInputKeyCodecClass(LongCodec.class)
        .setInputValueCodecClass(MLRDataCodec.class)
        .setModelUpdateFunctionClass(MLRETModelUpdateFunction.class)
        .setModelKeyCodecClass(IntegerCodec.class)
        .setModelValueCodecClass(DenseVectorCodec.class)
        .setModelUpdateValueCodecClass(DenseVectorCodec.class)
        .addParameterClass(NumClasses.class)
        .addParameterClass(InitialStepSize.class)
        .build());
  }
}
