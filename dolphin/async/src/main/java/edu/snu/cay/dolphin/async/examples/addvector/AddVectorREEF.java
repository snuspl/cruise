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
package edu.snu.cay.dolphin.async.examples.addvector;

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.examples.param.ExampleParameters;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorSerializer;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Application launching code for AddVectorREEF.
 */
public final class AddVectorREEF {

  /**
   * Should not be instantiated.
   */
  private AddVectorREEF() {
  }

  /**
   * Runs app with given arguments.
   * @param args command line arguments for running app
   * @return a LauncherStatus
   */
  public static LauncherStatus runAddVector(final String[] args) {
    return AsyncDolphinLauncher.launch("AddVectorREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setTrainerClass(AddVectorTrainer.class)
        .setUpdaterClass(AddVectorUpdater.class)
        .setValueCodecClass(DenseVectorCodec.class)
        .setServerSerializerClass(DenseVectorSerializer.class)
        .addParameterClass(ExampleParameters.DeltaValue.class)
        .addParameterClass(ExampleParameters.NumKeys.class)
        .addParameterClass(ExampleParameters.NumWorkers.class)
        .addParameterClass(ExampleParameters.ComputeTimeMs.class)
        .addParameterClass(VectorSize.class)
        .build());
  }

  public static void main(final String[] args) {
    runAddVector(args);
  }

  @NamedParameter(doc = "The size of vector", short_name = "vector_size")
  final class VectorSize implements Name<Integer> {
  }
}
