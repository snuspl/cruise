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
package edu.snu.cay.dolphin.async.examples.addinteger;

import edu.snu.cay.dolphin.async.ETDolphinConfiguration;
import edu.snu.cay.dolphin.async.ETDolphinLauncher;
import edu.snu.cay.dolphin.async.examples.common.ExampleDataParser;
import edu.snu.cay.dolphin.async.examples.common.ExampleParameters;
import edu.snu.cay.dolphin.async.mlapps.serialization.IntegerCodec;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

/**
 * Application launching code for AddIntegerREEF.
 */
public final class AddIntegerET {

  /**
   * Should not be instantiated.
   */
  private AddIntegerET() {
  }

  /**
   * Runs app with given arguments and custom configuration.
   * @param args command line arguments for running app
   * @param conf a job configuration
   * @return a LauncherStatus
   */
  public static LauncherStatus runAddInteger(final String[] args, final Configuration conf) {
    return ETDolphinLauncher.launch("AddIntegerET", args, ETDolphinConfiguration.newBuilder()
        .setTrainerClass(AddIntegerTrainer.class)
        .setInputParserClass(ExampleDataParser.class)
        .setModelKeyCodecClass(IntegerCodec.class)
        .setModelValueCodecClass(IntegerCodec.class)
        .setModelUpdateFunctionClass(AddIntegerUpdateFunction.class)
        .addParameterClass(ExampleParameters.DeltaValue.class)
        .addParameterClass(ExampleParameters.NumKeys.class)
        .addParameterClass(ExampleParameters.ComputeTimeMs.class)
        .addParameterClass(ExampleParameters.NumTrainingData.class)
        .addParameterClass(ExampleParameters.NumTestData.class)
        .addParameterClass(ExampleParameters.NumWorkers.class)
        .build(), conf);
  }

  public static void main(final String[] args) {
    runAddInteger(args, Tang.Factory.getTang().newConfigurationBuilder().build());
  }
}
