/**
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
package edu.snu.cay.dolphin.examples.ml.algorithms.clustering.kmeans;

import edu.snu.cay.dolphin.examples.ml.parameters.MaxIterations;
import edu.snu.cay.dolphin.core.UserParameters;
import edu.snu.cay.dolphin.examples.ml.parameters.ConvergenceThreshold;
import edu.snu.cay.dolphin.examples.ml.parameters.NumberOfClusters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

public final class KMeansParameters implements UserParameters {
  private final double convThreshold;
  private final int maxIterations;
  private final int numberOfClusters;

  @Inject
  private KMeansParameters(@Parameter(ConvergenceThreshold.class) final double convThreshold,
                           @Parameter(MaxIterations.class) final int maxIterations,
                           @Parameter(NumberOfClusters.class) final int numberOfClusters) {
    this.convThreshold = convThreshold;
    this.maxIterations = maxIterations;
    this.numberOfClusters = numberOfClusters;
  }

  @Override
  public Configuration getDriverConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ConvergenceThreshold.class, String.valueOf(convThreshold))
        .bindNamedParameter(MaxIterations.class, String.valueOf(maxIterations))
        .bindNamedParameter(NumberOfClusters.class, String.valueOf(numberOfClusters))
        .build();
  }

  @Override
  public Configuration getServiceConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .build();
  }

  @Override
  public Configuration getUserCmpTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .build();
  }

  @Override
  public Configuration getUserCtrlTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ConvergenceThreshold.class, String.valueOf(convThreshold))
        .bindNamedParameter(MaxIterations.class, String.valueOf(maxIterations))
        .bindNamedParameter(NumberOfClusters.class, String.valueOf(numberOfClusters))
        .build();
  }

  public static CommandLine getCommandLine() {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(ConvergenceThreshold.class);
    cl.registerShortNameOfClass(MaxIterations.class);
    cl.registerShortNameOfClass(NumberOfClusters.class);
    return cl;
  }
}
