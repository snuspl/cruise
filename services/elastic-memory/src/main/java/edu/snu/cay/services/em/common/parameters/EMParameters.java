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
package edu.snu.cay.services.em.common.parameters;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Configures the parameters used in Elastic Memory.
 */
public final class EMParameters {
  private final int numTotalBlocks;
  private final int numStoreThreads;

  @Inject
  private EMParameters(@Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                       @Parameter(NumStoreThreads.class) final int numStoreThreads) {
    this.numTotalBlocks = numTotalBlocks;
    this.numStoreThreads = numStoreThreads;
  }

  /**
   * @return A fully-configured Tang Configuration given the instantiated EMParameters.
   *         This configuration should be used to launch the Driver.
   */
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, String.valueOf(numTotalBlocks))
        .bindNamedParameter(NumStoreThreads.class, String.valueOf(numStoreThreads))
        .build();
  }

  /**
   * Register all short names to the command line parser, for use at the client.
   * @param commandLine The CommandLine instantiated at the client.
   * @return The CommandLine after short names are registered.
   */
  public static CommandLine registerShortNames(final CommandLine commandLine) {
    return commandLine.registerShortNameOfClass(NumTotalBlocks.class)
        .registerShortNameOfClass(NumStoreThreads.class);
  }
}
