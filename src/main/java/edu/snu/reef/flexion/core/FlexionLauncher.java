package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.nggroup.impl.driver.GroupCommService;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class FlexionLauncher {
  private final static Logger LOG = Logger.getLogger(FlexionLauncher.class.getName());

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
    return DriverLauncher.getLauncher(flexionParameters.getOnLocal() ? getLocalRutimeConfiguration() : getYarnRuntimeConfiguration())
        .run(getDriverConfiguration(), flexionParameters.getTimeout());
  }

  private final Configuration getLocalRutimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, flexionParameters.getEvalNum())
        .build();
  }

  private final Configuration getYarnRuntimeConfiguration() {
    return YarnClientConfiguration.CONF.build();
  }

  private final Configuration getDriverConfiguration() {
    final Configuration driverConfiguration = DriverConfiguration.CONF
        // .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(KMeansDriver.class))
        // .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TextInputFormat.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, flexionParameters.getIdentifier())
        // .set(DriverConfiguration.ON_DRIVER_STARTED, AlsDriver.StartHandler.class)
        // .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, AlsDriver.AllocatedEvaluatorHandler.class)
        // .set(DriverConfiguration.ON_CONTEXT_ACTIVE, AlsDriver.ActiveContextHandler.class)
        .build();

    return Configurations.merge(driverConfiguration,
                                GroupCommService.getConfiguration());
                                // alsParameters.getDriverConfiguration());
  }

}
