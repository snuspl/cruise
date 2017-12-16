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
package edu.snu.spl.cruise.ps.mlapps.lasso;

import edu.snu.spl.cruise.ps.core.client.CruisePSConfiguration;
import edu.snu.spl.cruise.ps.core.client.CruisePSLauncher;
import edu.snu.spl.cruise.ps.mlapps.serialization.DenseVectorCodec;
import edu.snu.spl.cruise.utils.StreamingSerializableCodec;

import javax.inject.Inject;

/**
 * Application launching code for LassoET.
 */
public final class LassoET {

  @Inject
  private LassoET() {
  }

  public static void main(final String[] args) {
    CruisePSLauncher.launch("LassoET", args, CruisePSConfiguration.newBuilder()
        .setTrainerClass(LassoTrainer.class)
        .setInputParserClass(LassoParser.class)
        .setInputKeyCodecClass(StreamingSerializableCodec.class)
        .setInputValueCodecClass(LassoDataCodec.class)
        .setModelUpdateFunctionClass(LassoModelUpdateFunction.class)
        .setModelKeyCodecClass(StreamingSerializableCodec.class)
        .setModelValueCodecClass(DenseVectorCodec.class)
        .setModelUpdateValueCodecClass(DenseVectorCodec.class)
        .build());
  }
}
