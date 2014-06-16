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
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import org.apache.reef.inmemory.fs.DfsParameters;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

/**
 * Launcher for InMemory Application
 */
public class Launch
{
  /**
   * Logger Object for System Log.
   */
  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

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
            .registerShortNameOfClass(MetaServerParameters.Port.class)
            .registerShortNameOfClass(MetaServerParameters.Timeout.class)
            .registerShortNameOfClass(MetaServerParameters.Threads.class)
            .registerShortNameOfClass(DfsParameters.Type.class)
            .registerShortNameOfClass(DfsParameters.Address.class)
            .processCommandLine(args);
    return confBuilder.build();
  }

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

  private static Configuration getInMemoryConfiguration(final Configuration clConf)
          throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(clConf);
    final Configuration inMemoryConfig = InMemoryConfiguration.getConf(injector.getNamedInstance(DfsParameters.Type.class))
            .set(InMemoryConfiguration.METASERVER_PORT, injector.getNamedInstance(MetaServerParameters.Port.class))
            .set(InMemoryConfiguration.DFS_TYPE, injector.getNamedInstance(DfsParameters.Type.class))
            .set(InMemoryConfiguration.DFS_ADDRESS, injector.getNamedInstance(DfsParameters.Address.class))
            .build();
    return inMemoryConfig;
 }

  /** 
   * Run InMemory Application
   */
  private static void runInMemory(final Configuration runtimeConf, final Configuration clConfig) throws InjectionException {
    final REEF reef = Tang.Factory.getTang().newInjector(runtimeConf).getInstance(REEFImplementation.class);
    final Configuration driverConfig = getDriverConfiguration();
    final Configuration inMemoryConfig = getInMemoryConfiguration(clConfig);
    reef.submit(Tang.Factory.getTang().newConfigurationBuilder(driverConfig, inMemoryConfig).build());
  }


  public static void main(String[] args) throws BindException, InjectionException, IOException {
    final Configuration runtimeConfig = getRuntimeConfiguration();
    final Configuration clConfig = parseCommandLine(args);
    runInMemory(runtimeConfig, clConfig);
    LOG.log(Level.INFO, "Job Submitted");
  }

}