package org.apache.reef.inmemory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;

/**
 * Launcher for InMemory Application
 */
public class Launch
{
  /**
   * Logger Object for System Log.
   */
  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  @NamedParameter(doc = "InMemory Cache Driver hostname",
          short_name = "hostname", default_value = "localhost")
  public static final class Hostname implements Name<String> {
  }

  @NamedParameter(doc = "InMemory Cache Driver port", short_name = "port", default_value = "18000")
  public static final class Port implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory Cache Driver timeout", short_name = "timeout", default_value = "30000")
  public static final class Timeout implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory Cache Driver threads", short_name = "num_threads", default_value = "10")
  public static final class NumThreads implements Name<Integer> {
  }

  /**
   * Build Runtime Configuration
   */ 
  private static final Configuration getRuntimeConfiguration() throws BindException {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
        .build();
  }

  /**
   * Parse the command line arguments.
   */
  private static Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder confBuilder =
            Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder)
            .registerShortNameOfClass(Hostname.class)
            .registerShortNameOfClass(Port.class)
            .registerShortNameOfClass(Timeout.class)
            .registerShortNameOfClass(NumThreads.class)
            .processCommandLine(args);
    return confBuilder.build();
  }

  // TODO: Do we need to clone?

  /**
   * Build Driver Configuration
   */ 
  private static Configuration getDriverConfiguration() {
    final Configuration driverConfig =
        EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "InMemory")
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, InMemoryDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, InMemoryDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, InMemoryDriver.StartHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, InMemoryDriver.TaskMessageHandler.class)
        .build();
    return driverConfig;
  }

  /** 
   * Run InMemory Application
   */
  private static void runInMemory(final Configuration runtimeConf) throws InjectionException {
    final REEF reef = Tang.Factory.getTang().newInjector(runtimeConf).getInstance(REEFImplementation.class);
    final Configuration driverConfig = getDriverConfiguration();
    reef.submit(driverConfig);
  }


  public static void main(String[] args) throws BindException, InjectionException, IOException {
    final Configuration runtimeConfig = getRuntimeConfiguration();
    final Configuration clConfig = parseCommandLine(args);
    runInMemory(Tang.Factory.getTang().newConfigurationBuilder(runtimeConfig, clConfig).build());
    LOG.log(Level.INFO, "Job Submitted");
  }

}