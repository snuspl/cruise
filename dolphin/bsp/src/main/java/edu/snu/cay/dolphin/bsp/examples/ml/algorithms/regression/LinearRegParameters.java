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
package edu.snu.cay.dolphin.bsp.examples.ml.algorithms.regression;

import edu.snu.cay.common.param.Parameters.Iterations;
import edu.snu.cay.dolphin.bsp.examples.ml.loss.Loss;
import edu.snu.cay.dolphin.bsp.core.UserParameters;
import edu.snu.cay.dolphin.bsp.examples.ml.data.RowSerializer;
import edu.snu.cay.dolphin.bsp.examples.ml.loss.SquareLoss;
import edu.snu.cay.dolphin.bsp.examples.ml.parameters.*;
import edu.snu.cay.dolphin.bsp.examples.ml.regularization.L2Regularization;
import edu.snu.cay.dolphin.bsp.examples.ml.regularization.Regularization;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

public final class LinearRegParameters implements UserParameters {
  private final double convThreshold;
  private final double stepSize;
  private final double lambda;
  private final int maxIterations;
  private final int dimension;

  @Inject
  private LinearRegParameters(@Parameter(ConvergenceThreshold.class) final double convThreshold,
                              @Parameter(StepSize.class) final double stepSize,
                              @Parameter(Lambda.class) final double lambda,
                              @Parameter(Dimension.class) final int dimension,
                              @Parameter(Iterations.class) final int maxIterations) {
    this.convThreshold = convThreshold;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.dimension = dimension;
    this.maxIterations = maxIterations;
  }

  @Override
  public Configuration getDriverConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ConvergenceThreshold.class, String.valueOf(convThreshold))
        .bindNamedParameter(StepSize.class, String.valueOf(stepSize))
        .bindNamedParameter(Dimension.class, String.valueOf(dimension))
        .bindNamedParameter(Lambda.class, String.valueOf(lambda))
        .bindNamedParameter(Iterations.class, String.valueOf(maxIterations))
        .bindImplementation(Loss.class, SquareLoss.class)
        .bindImplementation(Regularization.class, L2Regularization.class)
        .build();
  }

  @Override
  public Configuration getServiceConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Dimension.class, String.valueOf(dimension))
        .bindImplementation(Serializer.class, RowSerializer.class)
        .bindNamedParameter(IsDenseVector.class, String.valueOf(true))
        .build();
  }

  @Override
  public Configuration getUserCmpTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(StepSize.class, String.valueOf(stepSize))
        .bindNamedParameter(Lambda.class, String.valueOf(lambda))
        .bindImplementation(Loss.class, SquareLoss.class)
        .bindImplementation(Regularization.class, L2Regularization.class)
        .build();
  }

  @Override
  public Configuration getUserCtrlTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ConvergenceThreshold.class, String.valueOf(convThreshold))
        .bindNamedParameter(Iterations.class, String.valueOf(maxIterations))
        .build();
  }

  public static CommandLine getCommandLine() {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(StepSize.class);
    cl.registerShortNameOfClass(Dimension.class);
    cl.registerShortNameOfClass(Lambda.class);
    cl.registerShortNameOfClass(Iterations.class);
    cl.registerShortNameOfClass(ConvergenceThreshold.class);
    return cl;
  }
}
