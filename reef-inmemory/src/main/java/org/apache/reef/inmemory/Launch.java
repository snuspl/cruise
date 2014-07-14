package org.apache.reef.inmemory;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.reef.webserver.HttpEventHandlers;
import com.microsoft.reef.webserver.HttpHandlerConfiguration;
import com.microsoft.tang.*;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.tang.formats.ConfigurationModule;
import org.apache.reef.inmemory.client.YarnMetaserverResolver;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.inmemory.common.InMemoryConfiguration;
import org.apache.reef.inmemory.driver.InMemoryDriver;
import org.apache.reef.inmemory.driver.service.InetServiceRegistry;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;
import org.apache.reef.inmemory.driver.service.ServiceRegistry;
import org.apache.reef.inmemory.driver.service.YarnServiceRegistry;
import org.apache.reef.inmemory.task.CacheParameters;

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

  @NamedParameter(doc = "Number of threads to support for local runtime",
    short_name = "local_threads", default_value = "2")
  public static final class LocalThreads implements Name<Integer> {
  }

  /**
   * Parse the command line arguments.
   */
  public static Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder confBuilder =
      Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(LocalThreads.class);
    cl.registerShortNameOfClass(MetaServerParameters.Port.class);
    cl.registerShortNameOfClass(MetaServerParameters.InitCacheServers.class);
    cl.registerShortNameOfClass(MetaServerParameters.DefaultMemCacheServers.class);
    cl.registerShortNameOfClass(MetaServerParameters.Timeout.class);
    cl.registerShortNameOfClass(MetaServerParameters.Threads.class);
    cl.registerShortNameOfClass(CacheParameters.Port.class);
    cl.registerShortNameOfClass(CacheParameters.NumServerThreads.class);
    cl.registerShortNameOfClass(CacheParameters.NumLoadingThreads.class);
    cl.registerShortNameOfClass(DfsParameters.Type.class);
    cl.registerShortNameOfClass(DfsParameters.Address.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  /**
   * Build Driver Configuration
   */
  private static Configuration getDriverConfiguration(final Configuration clConfig) throws InjectionException {
    // Common
    final Configuration driverCommonConfig =
      EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "InMemory")
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, InMemoryDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, InMemoryDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, InMemoryDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, InMemoryDriver.StartHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, InMemoryDriver.TaskMessageHandler.class)
        .build();

    // Per-runtime configurations
    final Injector injector = Tang.Factory.getTang().newInjector(clConfig);
    final boolean isLocal = injector.getNamedInstance(Local.class);
    final Configuration driverConfig;
    if(isLocal) {
      final Configuration driverRegistryConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bind(ServiceRegistry.class, InetServiceRegistry.class)
        .build();
      driverConfig = Configurations.merge(driverCommonConfig, driverRegistryConfig);
    } else {
      final Configuration driverRegistryConfig = Tang.Factory.getTang().newConfigurationBuilder()
        .bind(ServiceRegistry.class, YarnServiceRegistry.class)
        .build();
      final Configuration driverHttpConfig = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, YarnServiceRegistry.AddressHttpHandler.class)
        .build();
      driverConfig = Configurations.merge(driverCommonConfig, driverRegistryConfig, driverHttpConfig);
    }
    // TODO: make ConfigurationModule for driverRegistryConfig?

    return driverConfig;
  }

  private static Configuration getInMemoryConfiguration(final Configuration clConf)
    throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(clConf);
    final Configuration inMemoryConfig;
    inMemoryConfig = InMemoryConfiguration.getConf(injector.getNamedInstance(DfsParameters.Type.class))
      .set(InMemoryConfiguration.METASERVER_PORT, injector.getNamedInstance(MetaServerParameters.Port.class))
      .set(InMemoryConfiguration.INIT_CACHE_SERVERS, injector.getNamedInstance(MetaServerParameters.InitCacheServers.class))
      .set(InMemoryConfiguration.DEFAULT_MEM_CACHE_SERVERS, injector.getNamedInstance(MetaServerParameters.DefaultMemCacheServers.class))
      .set(InMemoryConfiguration.CACHESERVER_PORT, injector.getNamedInstance(CacheParameters.Port.class))
      .set(InMemoryConfiguration.CACHESERVER_SERVER_THREADS, injector.getNamedInstance(CacheParameters.NumServerThreads.class))
      .set(InMemoryConfiguration.CACHESERVER_LOADING_THREADS, injector.getNamedInstance(CacheParameters.NumLoadingThreads.class))
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
      final int localThreads = injector.getNamedInstance(LocalThreads.class);
      runtimeConfig = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, localThreads)
        .build();
    } else {
      runtimeConfig = YarnClientConfiguration.CONF.build();
    }
    return runtimeConfig;
  }

  /**
   * Run InMemory Application
   */
  public static REEF runInMemory(final Configuration clConfig) throws InjectionException {
    final Configuration driverConfig = getDriverConfiguration(clConfig);
    final Configuration inMemoryConfig = getInMemoryConfiguration(clConfig);
    final Configuration runtimeConfig = getRuntimeConfiguration(clConfig);
    final Injector injector = Tang.Factory.getTang().newInjector(runtimeConfig);
    final REEF reef = injector.getInstance(REEFImplementation.class);
    reef.submit(Tang.Factory.getTang().newConfigurationBuilder(driverConfig, inMemoryConfig).build());
    return reef;
  }

  public static void main(String[] args) throws BindException, InjectionException, IOException {
    final Configuration clConfig = parseCommandLine(args);
    runInMemory(clConfig);
    LOG.log(Level.INFO, "Job Submitted");
  }

}