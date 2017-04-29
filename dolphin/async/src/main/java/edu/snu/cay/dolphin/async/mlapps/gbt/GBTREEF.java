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
package edu.snu.cay.dolphin.async.mlapps.gbt;


import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.mlapps.serialization.*;

import static edu.snu.cay.dolphin.async.mlapps.gbt.GBTParameters.*;

/**
 * Application launching code for GBTREEF.
 */
public final class GBTREEF {

  private GBTREEF() {
  }

  public static void main(final String[] args) {
    AsyncDolphinLauncher.launch("GBTREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setTrainerClass(GBTTrainer.class)
        .setUpdaterClass(GBTUpdater.class)
        .setParserClass(GBTDataParser.class)
        .setTestDataParserClass(GBTETDataParser.class)
        .setPreValueCodecClass(GBTreeCodec.class)
        .setValueCodecClass(GBTreeListCodec.class)
        .setServerSerializerClass(GBTreeListSerializer.class)
        .setWorkerSerializerClass(GBTDataSerializer.class)
        .addParameterClass(NumFeatures.class)
        .addParameterClass(StepSize.class)
        .addParameterClass(Lambda.class)
        .addParameterClass(Gamma.class)
        .addParameterClass(TreeMaxDepth.class)
        .addParameterClass(LeafMinSize.class)
        .addParameterClass(MetadataPath.class)
        .addParameterClass(NumKeys.class)
        .build());
  }
}
