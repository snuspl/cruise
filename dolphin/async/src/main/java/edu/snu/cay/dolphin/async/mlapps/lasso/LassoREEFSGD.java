/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.NullDataParser;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorSerializer;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import static edu.snu.cay.dolphin.async.mlapps.lasso.LassoParameters.*;

/**
 * Application launching code for LassoREEF.
 */
public final class LassoREEFSGD {

  /**
   * Should not be instantiated.
   */
  private LassoREEFSGD() {
  }

  public static void main(final String[] args) {
    AsyncDolphinLauncher.launch("LassoREEFSGD", args, AsyncDolphinConfiguration.newBuilder()
        .setTrainerClass(LassoTrainerSGD.class)
        .setUpdaterClass(LassoUpdater.class)
        .setParserClass(LassoParserSGD.class)
        .setPreValueCodecClass(DenseVectorCodec.class)
        .setValueCodecClass(DenseVectorCodec.class)
        .setServerSerializerClass(DenseVectorSerializer.class)
        .setWorkerSerializerClass(LassoDataSerializer.class)
        .addParameterClass(NumFeatures.class)
        .addParameterClass(NumFeaturesPerPartition.class)
        .addParameterClass(StepSize.class)
        .addParameterClass(Lambda.class)
        .build());
  }
}
