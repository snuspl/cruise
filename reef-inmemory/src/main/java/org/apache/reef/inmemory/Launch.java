package org.apache.reef.inmemory;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
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
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.fs.DfsParameters;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

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
   * Parameter for runtime configuration
   */
  @NamedParameter(doc = "Whether the application runs on local runtime",
    short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Parse the command line arguments.
   */
  private static Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder confBuilder =
      Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(MetaServerParameters.Port.class);
    cl.registerShortNameOfClass(MetaServerParameters.Timeout.class);
    cl.registerShortNameOfClass(MetaServerParameters.Threads.class);
    cl.registerShortNameOfClass(CacheParameters.Port.class);
    cl.registerShortNameOfClass(CacheParameters.NumThreads.class);
    cl.registerShortNameOfClass(DfsParameters.Type.class);
    cl.registerShortNameOfClass(DfsParameters.Address.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  /**
   * Build Driver Configuration
   */
  private static Configuration getDriverConfiguration() {
    final Configuration driverConfig;
    driverConfig = EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "InMemory")
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, InMemoryDriver.EvaluatorAllocatedHandler.class)
      .set(DriverConfiguration.ON_TASK_RUNNING, InMemoryDriver.RunningTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_COMPLETED, InMemoryDriver.CompletedTaskHandler.class)
      .set(DriverConfiguration.ON_DRIVER_STARTED, InMemoryDriver.StartHandler.class)
      .set(DriverConfiguration.ON_TASK_MESSAGE, InMemoryDriver.TaskMessageHandler.class)
      .build();
    return driverConfig;
  }

  private static Configuration getInMemoryConfiguration(final Configuration clConf)
    throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(clConf);
    final Configuration inMemoryConfig;
    inMemoryConfig = InMemoryConfiguration.getConf(injector.getNamedInstance(DfsParameters.Type.class))
      .set(InMemoryConfiguration.METASERVER_PORT, injector.getNamedInstance(MetaServerParameters.Port.class))
      .set(InMemoryConfiguration.CACHESERVER_PORT, injector.getNamedInstance(CacheParameters.Port.class))
      .set(InMemoryConfiguration.NUM_THREADS, injector.getNamedInstance(CacheParameters.NumThreads.class))
      .set(InMemoryConfiguration.DFS_TYPE, injector.getNamedInstance(DfsParameters.Type.class))
      .set(InMemoryConfiguration.DFS_ADDRESS, injector.getNamedInstance(DfsParameters.Address.class))
      .build();
    return inMemoryConfig;
  }

  /**
   * Build Runtime Configuration
   */
  private static Configuration getRuntimeConfiguration(final Configuration clConfig) throws BindException, InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(clConfig);
    final boolean isLocal = injector.getNamedInstance(Local.class);
    final Configuration runtimeConfig;
    if(isLocal) {
      runtimeConfig = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
        .build();
    } else {
      runtimeConfig = YarnClientConfiguration.CONF.build();
    }
    return runtimeConfig;
  }

  /**
   * Run InMemory Application
   */
  private static void runInMemory(final Configuration clConfig) throws InjectionException {
    final Configuration driverConfig = getDriverConfiguration();
    final Configuration inMemoryConfig = getInMemoryConfiguration(clConfig);
    final Configuration runtimeConfig = getRuntimeConfiguration(clConfig);
    final Injector injector = Tang.Factory.getTang().newInjector(runtimeConfig);
    final REEF reef = injector.getInstance(REEFImplementation.class);
    reef.submit(Tang.Factory.getTang().newConfigurationBuilder(driverConfig, inMemoryConfig).build());
  }

  public static void main(String[] args) throws BindException, InjectionException, IOException {
    final Configuration clConfig = parseCommandLine(args);
    runInMemory(clConfig);
    LOG.log(Level.INFO, "Job Submitted");
  }

}