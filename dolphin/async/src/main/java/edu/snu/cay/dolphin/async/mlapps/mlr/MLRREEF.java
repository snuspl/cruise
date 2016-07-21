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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorSerializer;

/**
 * Application launching code for MLRREEF.
 */
public final class MLRREEF {

  /**
   * Should not be instantiated.
   */
  private MLRREEF() {
  }

  public static void main(final String[] args) {
    AsyncDolphinLauncher.launch("MLRREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setWorkerClass(MLRWorker.class)
        .setUpdaterClass(MLRUpdater.class)
        .setPreValueCodecClass(DenseVectorCodec.class)
        .setValueCodecClass(DenseVectorCodec.class)
        .setServerSerializerClass(DenseVectorSerializer.class)
        .setWorkerSerializerClass(MLRDataSerializer.class)
        .addParameterClass(MLRParameters.NumClasses.class)
        .addParameterClass(MLRParameters.NumFeatures.class)
        .addParameterClass(MLRParameters.InitialStepSize.class)
        .addParameterClass(MLRParameters.Lambda.class)
        .addParameterClass(MLRParameters.StatusLogPeriod.class)
        .addParameterClass(MLRParameters.NumFeaturesPerPartition.class)
        .addParameterClass(MLRParameters.ModelGaussian.class)
        .addParameterClass(MLRParameters.DecayPeriod.class)
        .addParameterClass(MLRParameters.DecayRate.class)
        .addParameterClass(MLRParameters.TrainErrorDatasetSize.class)
        .addParameterClass(MLRParameters.NumBatchPerLossLog.class)
        .build());
  }
}
