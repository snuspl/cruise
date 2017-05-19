/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.async.optimizer.conf;

import edu.snu.cay.dolphin.async.optimizer.api.Optimizer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Configures the Optimizer class based on parameters.
 */
public final class OptimizerParameters {

  private final Class<? extends Optimizer> optimizerClass;

  @Inject
  private OptimizerParameters(@Parameter(OptimizerClass.class) final String optimizerClassString) {
    try {
      optimizerClass = (Class<? extends Optimizer>) Class.forName(optimizerClassString);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Reflection failed", e);
    }
  }

  /**
   * @return A fully-configured Tang Configuration given the instantiated OptimizerParameters.
   *         This configuration should be used to launch the Driver.
   */
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Optimizer.class, optimizerClass)
        .build();
  }

  /**
   * Register all short names to the command line parser, for use at the client.
   * @param commandLine The CommandLine instantiated at the client.
   * @return The CommandLine after short names are registered.
   */
  public static CommandLine registerShortNames(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(OptimizerClass.class);
  }
}
