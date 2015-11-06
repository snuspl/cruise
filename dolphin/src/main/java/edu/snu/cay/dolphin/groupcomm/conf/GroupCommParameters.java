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
package edu.snu.cay.dolphin.groupcomm.conf;

import org.apache.reef.io.network.group.api.driver.Topology;
import org.apache.reef.io.network.group.impl.config.parameters.TreeTopologyFanOut;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Configures the GroupComm parameters.
 * To be instantiated at both the Launcher and Driver.
 */
public final class GroupCommParameters {

  private final String topologyClassString;
  private final int fanOut;
  private final Class<? extends Topology> topologyClass;

  @Inject
  private GroupCommParameters(@Parameter(TopologyClassString.class) final String topologyClassString,
                              @Parameter(TreeTopologyFanOut.class) final int fanOut) {
    this.topologyClassString = topologyClassString;
    this.fanOut = fanOut;

    try {
      this.topologyClass = (Class<? extends Topology>) Class.forName(topologyClassString);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Reflection failed", e);
    }
  }

  /**
   * @return Topology used to create new communication groups
   *         To be used at the Driver.
   */
  public Class<? extends Topology> getTopologyClass() {
    return topologyClass;
  }

  /**
   * @return Fan out used to create new communication groups
   *         To be used at the Driver.
   */
  public int getFanOut() {
    return fanOut;
  }

  /**
   * @return A Tang Configuration that can be used to instantiate GroupCommParameters on the Driver.
   *         This configuration should be used to launch the Driver.
   */
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(TopologyClassString.class, topologyClassString)
        .bindNamedParameter(TreeTopologyFanOut.class, String.valueOf(fanOut))
        .build();
  }

  /**
   * Register all short names to the command line parser, for use at the client.
   * @param commandLine The CommandLine instantiated at the client.
   * @return The CommandLine after short names are registered.
   */
  public static CommandLine registerShortNames(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(TopologyClassString.class)
        .registerShortNameOfClass(TreeTopologyFanOut.class);
  }
}
