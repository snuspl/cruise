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
package edu.snu.spl.cruise.ps.mlapps.gbt;


import edu.snu.spl.cruise.ps.core.client.CruisePSConfiguration;
import edu.snu.spl.cruise.ps.core.client.CruisePSLauncher;
import edu.snu.spl.cruise.ps.mlapps.serialization.GBTreeCodec;
import edu.snu.spl.cruise.ps.mlapps.serialization.GBTreeListCodec;
import edu.snu.spl.cruise.utils.StreamingSerializableCodec;

import static edu.snu.spl.cruise.ps.mlapps.gbt.GBTParameters.*;

/**
 * Application launching code for GBTREEF.
 */
public final class GBTET {

  private GBTET() {
  }

  public static void main(final String[] args) {
    CruisePSLauncher.launch("GBTET", args, CruisePSConfiguration.newBuilder()
        .setTrainerClass(GBTTrainer.class)
        .setInputParserClass(GBTDataParser.class)
        .setInputKeyCodecClass(StreamingSerializableCodec.class)
        .setInputValueCodecClass(GBTDataCodec.class)
        .setModelUpdateFunctionClass(GBTModelUpdateFunction.class)
        .setModelKeyCodecClass(StreamingSerializableCodec.class)
        .setModelValueCodecClass(GBTreeListCodec.class)
        .setModelUpdateValueCodecClass(GBTreeCodec.class)
        .addParameterClass(Gamma.class)
        .addParameterClass(TreeMaxDepth.class)
        .addParameterClass(LeafMinSize.class)
        .addParameterClass(MetadataPath.class)
        .addParameterClass(NumKeys.class)
        .build());
  }
}
