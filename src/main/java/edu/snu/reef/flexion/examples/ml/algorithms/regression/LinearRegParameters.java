package edu.snu.reef.flexion.examples.ml.algorithms.regression;

import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.examples.ml.loss.Loss;
import edu.snu.reef.flexion.examples.ml.loss.SquareLoss;
import edu.snu.reef.flexion.examples.ml.parameters.*;
import edu.snu.reef.flexion.examples.ml.regularization.L2Regularization;
import edu.snu.reef.flexion.examples.ml.regularization.Regularization;
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
                              @Parameter(MaxIterations.class) final int maxIterations) {
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
        .bindNamedParameter(MaxIterations.class, String.valueOf(maxIterations))
        .bindImplementation(Loss.class, SquareLoss.class)
        .bindImplementation(Regularization.class, L2Regularization.class)
        .build();
  }

  @Override
  public Configuration getServiceConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Dimension.class, String.valueOf(dimension))
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
        .bindNamedParameter(MaxIterations.class, String.valueOf(maxIterations))
        .build();
  }

  public static CommandLine getCommandLine() {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(StepSize.class);
    cl.registerShortNameOfClass(Dimension.class);
    cl.registerShortNameOfClass(Lambda.class);
    cl.registerShortNameOfClass(MaxIterations.class);
    cl.registerShortNameOfClass(ConvergenceThreshold.class);
    return cl;
  }

}
