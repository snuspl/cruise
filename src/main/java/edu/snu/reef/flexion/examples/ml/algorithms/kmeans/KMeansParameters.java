package edu.snu.reef.flexion.examples.ml.algorithms.kmeans;

import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.examples.ml.parameters.ConvergenceThreshold;
import edu.snu.reef.flexion.examples.ml.parameters.MaxIterations;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

public final class KMeansParameters implements UserParameters {

    private final double convThreshold;
    private final int maxIterations;

    @Inject
    private KMeansParameters(@Parameter(ConvergenceThreshold.class) final double convThreshold,
                             @Parameter(MaxIterations.class) final int maxIterations) {
        this.convThreshold = convThreshold;
        this.maxIterations = maxIterations;
    }

    @Override
    public Configuration getDriverConf() {
        return Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(ConvergenceThreshold.class, String.valueOf(convThreshold))
                .bindNamedParameter(MaxIterations.class, String.valueOf(maxIterations))
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
                .build();
    }

    public static CommandLine getCommandLine() {
        final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
        final CommandLine cl = new CommandLine(cb);
        cl.registerShortNameOfClass(ConvergenceThreshold.class);
        cl.registerShortNameOfClass(MaxIterations.class);
        return cl;
    }

}
