package edu.snu.reef.flexion.examples.ml.algorithms.clustering.kmeans;

import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.examples.ml.parameters.ConvergenceThreshold;
import edu.snu.reef.flexion.examples.ml.parameters.MaxIterations;
import edu.snu.reef.flexion.examples.ml.parameters.NumberOfClusters;
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
