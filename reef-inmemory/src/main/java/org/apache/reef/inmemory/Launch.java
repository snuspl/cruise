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

/**
 * Launcher for InMemory Application
 */
public class Launch
{
  /**
   * Logger Object for System Log.
   */
  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  @NamedParameter(doc = "InMemory Cache Driver port", short_name = "metaserver_port", default_value = "18000")
  public static final class MetaserverPort implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory Cache Driver timeout", short_name = "timeout", default_value = "30000")
  public static final class Timeout implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory Cache Driver threads", short_name = "num_threads", default_value = "10")
  public static final class NumThreads implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory Cache Task port", short_name = "cache_port", default_value = "18001")
  public static final class CachePort implements Name<Integer> {
  }

  @NamedParameter(doc = "Underlying DFS address", short_name = "dfs_address", default_value = "hdfs://localhost:50070")
  public static final class DfsAddress implements Name<String> {
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
            .registerShortNameOfClass(MetaserverPort.class)
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

  private static Configuration getInMemoryConfiguration(final Configuration runtimeConf)
          throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(runtimeConf);
    final Configuration inMemoryConfig = InMemoryConfiguration.HDFS_CONF
            .set(InMemoryConfiguration.METASERVER_PORT, injector.getNamedInstance(MetaserverPort.class))
            .set(InMemoryConfiguration.CACHE_PORT, injector.getNamedInstance(CachePort.class))
            .set(InMemoryConfiguration.DFS_ADDRESS, injector.getNamedInstance(DfsAddress.class))
            .build();
    return inMemoryConfig;
 }


  /** 
   * Run InMemory Application
   */
  private static void runInMemory(final Configuration runtimeConf) throws InjectionException {
    final REEF reef = Tang.Factory.getTang().newInjector(runtimeConf).getInstance(REEFImplementation.class);
    final Configuration driverConfig = getDriverConfiguration();
    final Configuration inMemoryConfig = getInMemoryConfiguration(runtimeConf);
    reef.submit(Tang.Factory.getTang().newConfigurationBuilder(driverConfig, inMemoryConfig).build());
  }


  public static void main(String[] args) throws BindException, InjectionException, IOException {
    final Configuration runtimeConfig = getRuntimeConfiguration();
    final Configuration clConfig = parseCommandLine(args);
    runInMemory(Tang.Factory.getTang().newConfigurationBuilder(runtimeConfig, clConfig).build());
    LOG.log(Level.INFO, "Job Submitted");
  }

}