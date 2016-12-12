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
package edu.snu.cay.dolphin.async.tensorflow.sample_example;

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.NullDataParser;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorSerializer;
import edu.snu.cay.dolphin.async.tensorflow.TensorflowParameters;
import org.apache.reef.client.LauncherStatus;

/**
 * Application launching code for TensorflowREEF.
 */
public final class TensorflowREEF {

  /**
   * Should not be instantiated.
   */
  private TensorflowREEF() {
  }

  /**
   * Runs app with given arguments.
   * @param args command line arguments for running app
   * @return a LauncherStatus
   */
  public static LauncherStatus runTensorflow(final String[] args) {
    return AsyncDolphinLauncher.launch("TensorflowREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setTrainerClass(TensorflowTrainer.class)
        .setUpdaterClass(TensorflowUpdater.class)
        .setParserClass(NullDataParser.class)
        .setPreValueCodecClass(DenseVectorCodec.class)
        .setValueCodecClass(DenseVectorCodec.class)
        .setServerSerializerClass(DenseVectorSerializer.class)
        .addParameterClass(TensorflowParameters.UseGPU.class)
        .build());
  }

  public static void main(final String[] args) {
    runTensorflow(args);
  }

}
