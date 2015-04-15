package edu.snu.reef.flexion.examples.ml.algorithms.clustering.em;

import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.examples.ml.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

public final class EMParameters implements UserParameters {

  private final double convThreshold;
  private final int maxIterations;
  private final int numberOfClusters;
  private final boolean isCovarianceDiagonal;
  private final boolean isCovarianceShared;

  @Inject
  private EMParameters(@Parameter(ConvergenceThreshold.class) final double convThreshold,
                       @Parameter(MaxIterations.class) final int maxIterations,
                       @Parameter(NumberOfClusters.class) final int numberOfClusters,
                       @Parameter(IsCovarianceDiagonal.class) final boolean isCovarianceDiagonal,
                       @Parameter(IsCovarianceShared.class) final boolean isCovarianceShared) {
    this.convThreshold = convThreshold;
    this.maxIterations = maxIterations;
    this.numberOfClusters = numberOfClusters;
    this.isCovarianceDiagonal = isCovarianceDiagonal;
    this.isCovarianceShared = isCovarianceShared;
  }

  @Override
  public Configuration getDriverConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ConvergenceThreshold.class, String.valueOf(convThreshold))
        .bindNamedParameter(MaxIterations.class, String.valueOf(maxIterations))
        .bindNamedParameter(NumberOfClusters.class, String.valueOf(numberOfClusters))
        .bindNamedParameter(IsCovarianceDiagonal.class, String.valueOf(isCovarianceDiagonal))
        .bindNamedParameter(IsCovarianceShared.class, String.valueOf(isCovarianceShared))
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
        .bindNamedParameter(IsCovarianceDiagonal.class, String.valueOf(isCovarianceDiagonal))
        .bindNamedParameter(IsCovarianceShared.class, String.valueOf(isCovarianceShared))
        .build();
  }

  public static CommandLine getCommandLine() {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(ConvergenceThreshold.class);
    cl.registerShortNameOfClass(MaxIterations.class);
    cl.registerShortNameOfClass(NumberOfClusters.class);
    cl.registerShortNameOfClass(IsCovarianceDiagonal.class);
    cl.registerShortNameOfClass(IsCovarianceShared.class);
    return cl;
  }

}
