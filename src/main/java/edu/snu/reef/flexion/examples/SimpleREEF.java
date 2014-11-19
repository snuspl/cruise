package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.examples.parameters.OnLocal;
import edu.snu.reef.flexion.examples.parameters.Timeout;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SimpleREEF {
  private final static Logger LOG = Logger.getLogger(SimpleREEF.class.getName());

  private final boolean onLocal;
  private final int timeout;

  @Inject
  private SimpleREEF(@Parameter(OnLocal.class) final boolean onLocal,
                     @Parameter(Timeout.class) final int timeout) {
    this.onLocal = onLocal;
    this.timeout = timeout;
  }

  private final static SimpleREEF parseCommandLine(String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);

    cl.registerShortNameOfClass(OnLocal.class);
    cl.registerShortNameOfClass(Timeout.class);

    cl.processCommandLine(args);

    return Tang.Factory.getTang().newInjector(cb.build()).getInstance(SimpleREEF.class);
  }

  public final static void main(String[] args) {
    LauncherStatus status;
    try {
      SimpleREEF simpleREEF = parseCommandLine(args);
      status = simpleREEF.run();
    } catch (final Exception e) {
      status = LauncherStatus.FAILED(e);
    }

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  private final LauncherStatus run() throws InjectionException {
    return DriverLauncher.getLauncher(onLocal?  getLocalRutimeConfiguration() : getYarnRuntimeConfiguration())
        .run(getDriverConfiguration(), timeout);
  }

  private final Configuration getLocalRutimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 4)
        .build();
  }

  private final Configuration getYarnRuntimeConfiguration() {
    return YarnClientConfiguration.CONF.build();
  }

  private final Configuration getDriverConfiguration() {
    final Configuration driverConfiguration = DriverConfiguration.CONF
        // .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(KMeansDriver.class))
        // .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TextInputFormat.class))
        // .set(DriverConfiguration.ON_DRIVER_STARTED, AlsDriver.StartHandler.class)
        // .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, AlsDriver.AllocatedEvaluatorHandler.class)
        // .set(DriverConfiguration.ON_CONTEXT_ACTIVE, AlsDriver.ActiveContextHandler.class)
        .build();

    return Configurations.merge(driverConfiguration,
        GroupCommService.getConfiguration(),
        alsParameters.getDriverConfiguration());
  }
}
