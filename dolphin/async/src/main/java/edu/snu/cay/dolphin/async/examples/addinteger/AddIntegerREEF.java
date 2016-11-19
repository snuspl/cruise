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
package edu.snu.cay.dolphin.async.examples.addinteger;

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.NullDataParser;
import edu.snu.cay.dolphin.async.examples.param.ExampleParameters;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Application launching code for AddIntegerREEF.
 */
public final class AddIntegerREEF {

  /**
   * Should not be instantiated.
   */
  private AddIntegerREEF() {
  }

  /**
   * Runs app with given arguments and custom configuration.
   * @param args command line arguments for running app
   * @param conf a job configuration
   * @return a LauncherStatus
   */
  public static LauncherStatus runAddInteger(final String[] args, final Configuration conf) {
    return AsyncDolphinLauncher.launch("AddIntegerREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setTrainerClass(AddIntegerTrainer.class)
        .setUpdaterClass(AddIntegerUpdater.class)
        .setParserClass(NullDataParser.class)
        .addParameterClass(ExampleParameters.DeltaValue.class)
        .addParameterClass(ExampleParameters.NumKeys.class)
        .addParameterClass(NumUpdatesPerItr.class)
        .addParameterClass(ExampleParameters.NumWorkers.class)
        .addParameterClass(ExampleParameters.ComputeTimeMs.class)
        .build(), conf);
  }

  public static void main(final String[] args) {
    runAddInteger(args, Tang.Factory.getTang().newConfigurationBuilder().build());
  }

  @NamedParameter(doc = "The number of updates for each key in an iteration", short_name = "num_updates")
  final class NumUpdatesPerItr implements Name<Integer> {
  }
}
