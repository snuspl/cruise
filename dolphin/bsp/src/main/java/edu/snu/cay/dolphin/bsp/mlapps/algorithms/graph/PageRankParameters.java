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
package edu.snu.cay.dolphin.bsp.mlapps.algorithms.graph;

import edu.snu.cay.common.param.Parameters.Iterations;
import edu.snu.cay.dolphin.bsp.core.UserParameters;
import edu.snu.cay.dolphin.bsp.mlapps.data.AdjacencyListSerializer;
import edu.snu.cay.dolphin.bsp.mlapps.parameters.ConvergenceThreshold;
import edu.snu.cay.dolphin.bsp.mlapps.parameters.DampingFactor;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

public final class PageRankParameters implements UserParameters {
  private final double convThreshold;
  private final double dampingFactor;
  private final int maxIterations;

  @Inject
  private PageRankParameters(@Parameter(ConvergenceThreshold.class) final double convThreshold,
                             @Parameter(DampingFactor.class) final double dampingFactor,
                             @Parameter(Iterations.class) final int maxIterations) {
    this.convThreshold = convThreshold;
    this.dampingFactor = dampingFactor;
    this.maxIterations = maxIterations;
  }

  @Override
  public Configuration getDriverConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ConvergenceThreshold.class, String.valueOf(convThreshold))
        .bindNamedParameter(DampingFactor.class, String.valueOf(dampingFactor))
        .bindNamedParameter(Iterations.class, String.valueOf(maxIterations))
        .build();
  }

  @Override
  public Configuration getServiceConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Serializer.class, AdjacencyListSerializer.class)
        .build();
  }

  @Override
  public Configuration getUserCmpTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder().build();
  }

  @Override
  public Configuration getUserCtrlTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ConvergenceThreshold.class, String.valueOf(convThreshold))
        .bindNamedParameter(DampingFactor.class, String.valueOf(dampingFactor))
        .bindNamedParameter(Iterations.class, String.valueOf(maxIterations))
        .build();
  }

  public static CommandLine getCommandLine() {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(ConvergenceThreshold.class);
    cl.registerShortNameOfClass(DampingFactor.class);
    cl.registerShortNameOfClass(Iterations.class);
    return cl;
  }
}
