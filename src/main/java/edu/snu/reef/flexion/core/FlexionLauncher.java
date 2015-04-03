package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.nggroup.impl.driver.GroupCommService;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Job launch code for the K-means Clustering REEF job
 */
public final class FlexionLauncher {
    private static final Logger LOG = Logger.getLogger(FlexionLauncher.class.getName());

    private final FlexionParameters flexionParameters;

    @Inject
    private FlexionLauncher(FlexionParameters flexionParameters) {
        this.flexionParameters = flexionParameters;
    }

    public final static void run(Configuration flexionConfig) {
        LauncherStatus status;
        try {
            status = Tang.Factory.getTang()
                    .newInjector(flexionConfig)
                    .getInstance(FlexionLauncher.class)
                    .run();
        } catch (final Exception e) {
            status = LauncherStatus.FAILED(e);
        }

        LOG.log(Level.INFO, "REEF job completed: {0}", status);

    }

    private final LauncherStatus run() throws InjectionException {
        return DriverLauncher.getLauncher(getRuntimeConfiguration())
                .run(getDriverConfWithDataLoad(), flexionParameters.getTimeout());
    }

    private final Configuration getRuntimeConfiguration() {
        return flexionParameters.getOnLocal()? getLocalRuntimeConfiguration() : getYarnRuntimeConfiguration();
    }

    private final Configuration getYarnRuntimeConfiguration() {
        return YarnClientConfiguration.CONF.build();
    }

    private final Configuration getLocalRuntimeConfiguration() {
        return LocalRuntimeConfiguration.CONF
                .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, flexionParameters.getEvalNum() + 1)
                .build();
    }

    private final Configuration getDriverConfWithDataLoad() {
        final ConfigurationModule driverConfiguration = DriverConfiguration.CONF
                .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(FlexionDriver.class))
                .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TextInputFormat.class))
                .set(DriverConfiguration.DRIVER_IDENTIFIER, flexionParameters.getIdentifier())
                .set(DriverConfiguration.ON_CONTEXT_ACTIVE, FlexionDriver.ActiveContextHandler.class)
                .set(DriverConfiguration.ON_TASK_MESSAGE, FlexionDriver.TaskMessageHandler.class)
                .set(DriverConfiguration.ON_TASK_COMPLETED, FlexionDriver.TaskCompletedHandler.class);
                //.set(DriverConfiguration.ON_TASK_FAILED, FlexionDriver.FailedTaskHandler.class);


        final EvaluatorRequest evalRequest = EvaluatorRequest.newBuilder()
                .setNumber(1)
                .setMemory(flexionParameters.getEvalSize())
                .build();

        final Configuration driverConfWithDataLoad = new DataLoadingRequestBuilder()
                .setMemoryMB(flexionParameters.getEvalSize())
                .setInputFormatClass(TextInputFormat.class)
                .setInputPath(processInputDir(flexionParameters.getInputDir()))
                .setNumberOfDesiredSplits(flexionParameters.getEvalNum())
                .setComputeRequest(evalRequest)
                .setDriverConfigurationModule(driverConfiguration)
                .build();

        return Configurations.merge(driverConfWithDataLoad,
                GroupCommService.getConfiguration(),
                flexionParameters.getDriverConf());

    }


    private final String processInputDir(final String inputDir) {
        if (!flexionParameters.getOnLocal()) {
            return inputDir;
        }

        final File inputFile = new File(inputDir);

        return "file:///" + inputFile.getAbsolutePath();
    }

}
