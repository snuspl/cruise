package org.apache.reef.inmemory;

import org.apache.commons.io.FileUtils;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.inmemory.common.Instrumentor;
import org.apache.reef.inmemory.common.InstrumentorImpl;
import org.apache.reef.inmemory.common.instrumentation.InstrumentationParameters;
import org.apache.reef.inmemory.common.instrumentation.ganglia.GangliaParameters;
import org.apache.reef.inmemory.driver.InMemoryDriver;
import org.apache.reef.inmemory.driver.InMemoryDriverConfiguration;
import org.apache.reef.inmemory.driver.locality.LocalLocationSorter;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.driver.locality.YarnLocationSorter;
import org.apache.reef.inmemory.driver.service.InetServiceRegistry;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;
import org.apache.reef.inmemory.driver.service.ServiceRegistry;
import org.apache.reef.inmemory.driver.service.YarnServiceRegistry;
import org.apache.reef.inmemory.task.CacheParameters;
import org.apache.reef.runtime.common.client.REEFImplementation;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.webserver.HttpHandlerConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launcher for InMemory Application.
 */
public final class Launch {
  /**
   * Logger Object for System Log.
   */
  private static final Logger LOG = Logger.getLogger(Launch.class.getName());

  /**
   * The file is located at "./conf/config.json" as a JSON format.
   */
  private static final String CONFIG_FILE = "conf/config.json";

  @NamedParameter(doc = "Whether the application runs on local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of maximum evaluators that can be created in the local runtime",
      short_name = "local_max_num_eval", default_value = "2")
  public static final class LocalMaxNumEval implements Name<Integer> {
  }

  // See: JVMHeapSlack class
  @NamedParameter(doc = "The fraction of the container memory NOT to use for the Java Heap.",
      short_name = "jvm_heap_slack", default_value = "0.05")
  public static class ReefJvmHeapSlack implements Name<Double>{
  }

  /**
   * File path of a replication rules JSON file.
   * The file will be read at the client and added to the server configuration as a String.
   */
  @NamedParameter(doc = "Replication rules JSON file path", short_name = "replication_rules")
  public static final class ReplicationRulesPath implements Name<String> {
  }

  /**
   * Parse the configuration file.
   * @return Configuration described in config file
   * @throws IOException If failed to parse the config file
   */
  public static Configuration parseConfigFile() throws IOException {
    return new AvroConfigurationSerializer().fromTextFile(new File(CONFIG_FILE));
  }

  /**
   * Parse the command line arguments.
   * @return Configuration given via command line
   * @throws IOException If failed to parse the command line
   */
  public static Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder confBuilder =
        Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(LocalMaxNumEval.class);
    cl.registerShortNameOfClass(ReefJvmHeapSlack.class);
    cl.registerShortNameOfClass(ReplicationRulesPath.class);
    cl.registerShortNameOfClass(MetaServerParameters.Port.class);
    cl.registerShortNameOfClass(MetaServerParameters.InitCacheServers.class);
    cl.registerShortNameOfClass(MetaServerParameters.DefaultMemCacheServers.class);
    cl.registerShortNameOfClass(MetaServerParameters.Timeout.class);
    cl.registerShortNameOfClass(MetaServerParameters.Threads.class);
    cl.registerShortNameOfClass(CacheParameters.Port.class);
    cl.registerShortNameOfClass(CacheParameters.NumServerThreads.class);
    cl.registerShortNameOfClass(CacheParameters.NumLoadingThreads.class);
    cl.registerShortNameOfClass(CacheParameters.Memory.class);
    cl.registerShortNameOfClass(CacheParameters.HeapSlack.class);
    cl.registerShortNameOfClass(DfsParameters.Type.class);
    cl.registerShortNameOfClass(DfsParameters.Address.class);
    cl.registerShortNameOfClass(InstrumentationParameters.InstrumentationReporterPeriod.class);
    cl.registerShortNameOfClass(GangliaParameters.Ganglia.class);
    cl.registerShortNameOfClass(GangliaParameters.GangliaHost.class);
    cl.registerShortNameOfClass(GangliaParameters.GangliaPort.class);
    cl.registerShortNameOfClass(GangliaParameters.GangliaPrefix.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  /**
   * Choose which configuration to use for each Parameter.
   * The arguments given from the command line overwrites the one from configuration file
   * @param clazz The Parameter class to set the value
   * @param clConfigInjector The injector of Command line configuration
   * @param fileConfigInjector The injector of Config file configuration
   * @return The instance for given Parameter
   * @throws InjectionException If failed to get instance
   */
  private static <T> T chooseNamedInstance(Class<? extends Name<T>> clazz, Injector clConfigInjector,
                                           Injector fileConfigInjector) throws InjectionException {
    return clConfigInjector.isParameterSet(clazz) ? clConfigInjector.getNamedInstance(clazz) :
        fileConfigInjector.getNamedInstance(clazz);
  }

  /**
   * Build Driver Configuration.
   */
  private static Configuration getDriverConfiguration() {
    final Configuration driverConfig;
    driverConfig = DriverConfiguration.CONF
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(InMemoryDriver.class))
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "InMemory")
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, InMemoryDriver.EvaluatorAllocatedHandler.class)
      .set(DriverConfiguration.ON_TASK_RUNNING, InMemoryDriver.RunningTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_COMPLETED, InMemoryDriver.CompletedTaskHandler.class)
      .set(DriverConfiguration.ON_DRIVER_STARTED, InMemoryDriver.StartHandler.class)
      .set(DriverConfiguration.ON_TASK_MESSAGE, InMemoryDriver.TaskMessageHandler.class)
      .build();
    return driverConfig;
  }

  private static ConfigurationModule setReplicationRules(final ConfigurationModule configModule,
                                                         final Injector clInjector,
                                                         final Injector fileInjector) {
    try {
      final String replicationRulesPath = chooseNamedInstance(ReplicationRulesPath.class, clInjector, fileInjector);
      final String replicationRules = FileUtils.readFileToString(new File(replicationRulesPath));
      LOG.log(Level.FINER, "Replication Rules: {0}", replicationRules);
      return configModule.set(InMemoryDriverConfiguration.REPLICATION_RULES, replicationRules);
    } catch (InjectionException e) {
      LOG.log(Level.FINE, "Replication Rules not set, will use default");
      return configModule;
    } catch (IOException e) {
      throw new BindException("Replication Rules could not be read", e);
    }
  }

  /**
   * Build InMemory Configuration which is used in application.
   */
  private static Configuration getInMemoryConfiguration(final Configuration clConf, final Configuration fileConf)
    throws InjectionException, BindException {
    final Injector clInjector = Tang.Factory.getTang().newInjector(clConf);
    final Injector fileInjector = Tang.Factory.getTang().newInjector(fileConf);

    ConfigurationModule inMemoryConfigModule = InMemoryDriverConfiguration
        .getConf(chooseNamedInstance(DfsParameters.Type.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.METASERVER_PORT,
            chooseNamedInstance(MetaServerParameters.Port.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.INIT_CACHE_SERVERS,
            chooseNamedInstance(MetaServerParameters.InitCacheServers.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.DEFAULT_MEM_CACHE_SERVERS,
            chooseNamedInstance(MetaServerParameters.DefaultMemCacheServers.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.CACHESERVER_PORT,
            chooseNamedInstance(CacheParameters.Port.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.CACHESERVER_SERVER_THREADS,
            chooseNamedInstance(CacheParameters.NumServerThreads.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.CACHESERVER_LOADING_THREADS,
            chooseNamedInstance(CacheParameters.NumLoadingThreads.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.CACHE_MEMORY_SIZE,
            chooseNamedInstance(CacheParameters.Memory.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.CACHESERVER_HEAP_SLACK,
            chooseNamedInstance(CacheParameters.HeapSlack.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.DFS_TYPE,
            chooseNamedInstance(DfsParameters.Type.class, clInjector, fileInjector))
        .set(InMemoryDriverConfiguration.DFS_ADDRESS,
            chooseNamedInstance(DfsParameters.Address.class, clInjector, fileInjector));
    inMemoryConfigModule = setReplicationRules(inMemoryConfigModule, clInjector, fileInjector);
    return inMemoryConfigModule.build();
  }

  /**
   * Build Runtime Configuration.
   * public for integration testing.
   */
  public static Configuration getRuntimeConfiguration(final Configuration clConf, final Configuration fileConf)
    throws BindException, InjectionException {
    final Injector clInjector = Tang.Factory.getTang().newInjector(clConf);
    final Injector fileInjector = Tang.Factory.getTang().newInjector(fileConf);

    final boolean isLocal = chooseNamedInstance(Local.class, clInjector, fileInjector);
    final Configuration runtimeConfig;
    if(isLocal) {
      final int localMaxNumEvals = chooseNamedInstance(LocalMaxNumEval.class, clInjector, fileInjector);
      runtimeConfig = LocalRuntimeConfiguration.CONF
              .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, localMaxNumEvals)
              .build();
    } else {
      final double jvmHeapSlack = chooseNamedInstance(ReefJvmHeapSlack.class, clInjector, fileInjector);
      runtimeConfig = YarnClientConfiguration.CONF
              .set(YarnClientConfiguration.JVM_HEAP_SLACK, jvmHeapSlack)
              .build();
    }
    return runtimeConfig;
  }

  private static Configuration getInstrumentationConfiguration(final Configuration clConf, final Configuration fileConf)
    throws InjectionException {
    final Injector clInjector = Tang.Factory.getTang().newInjector(clConf);
    final Injector fileInjector = Tang.Factory.getTang().newInjector(fileConf);

    final Instrumentor instrumentor = new InstrumentorImpl(
            chooseNamedInstance(InstrumentationParameters.InstrumentationReporterPeriod.class,
                clInjector, fileInjector),
            chooseNamedInstance(InstrumentationParameters.InstrumentationLogLevel.class, clInjector, fileInjector),
            chooseNamedInstance(GangliaParameters.Ganglia.class, clInjector, fileInjector),
            chooseNamedInstance(GangliaParameters.GangliaHost.class, clInjector, fileInjector),
            chooseNamedInstance(GangliaParameters.GangliaPort.class, clInjector, fileInjector),
            chooseNamedInstance(GangliaParameters.GangliaPrefix.class, clInjector, fileInjector));
    return instrumentor.getConfiguration();
  }

  /**
   * Build cluster-specific configuration.
   */
  private static Configuration getClusterConfiguration(final Configuration clConf, final Configuration fileConf)
    throws InjectionException, BindException {
    final Injector clInjector = Tang.Factory.getTang().newInjector(clConf);
    final Injector fileInjector = Tang.Factory.getTang().newInjector(fileConf);

    final Configuration clusterConfig;

    final boolean isLocal = chooseNamedInstance(Local.class, clInjector, fileInjector);
    if (isLocal) {
      final Configuration registryConfig = Tang.Factory.getTang().newConfigurationBuilder()
              .bind(ServiceRegistry.class, InetServiceRegistry.class)
              .bind(LocationSorter.class, LocalLocationSorter.class)
              .build();
      clusterConfig = Configurations.merge(registryConfig);
    } else {
      final Configuration registryConfig = Tang.Factory.getTang().newConfigurationBuilder()
              .bind(ServiceRegistry.class, YarnServiceRegistry.class)
              .bind(LocationSorter.class, YarnLocationSorter.class)
              .build();
      final Configuration httpConfig = HttpHandlerConfiguration.CONF
              .set(HttpHandlerConfiguration.HTTP_HANDLERS, YarnServiceRegistry.AddressHttpHandler.class)
              .build();
      clusterConfig = Configurations.merge(registryConfig, httpConfig);
    }
    return clusterConfig;
  }

  /**
   * Build launch configuration.
   * public for integration testing
   */
  public static Configuration getLaunchConfiguration(final Configuration clConfig, final Configuration fileConfig)
      throws InjectionException {
    final Configuration driverConfig = getDriverConfiguration();
    final Configuration inMemoryConfig = getInMemoryConfiguration(clConfig, fileConfig);
    final Configuration clusterConfig = getClusterConfiguration(clConfig, fileConfig);
    final Configuration instrumentationConfig = getInstrumentationConfiguration(clConfig, fileConfig);

    return Configurations.merge(driverConfig, inMemoryConfig, clusterConfig, instrumentationConfig);
  }

  /**
   * Run InMemory Application.
   */
  public static void runInMemory(final Configuration clConfig, final Configuration fileConfig)
      throws InjectionException {

    final Configuration runtimeConfig = getRuntimeConfiguration(clConfig, fileConfig);
    final Configuration launchConfig = getLaunchConfiguration(clConfig, fileConfig);
    final Injector injector = Tang.Factory.getTang().newInjector(runtimeConfig);

    final REEF reef = injector.getInstance(REEFImplementation.class);
    reef.submit(Tang.Factory.getTang().newConfigurationBuilder(launchConfig).build());
  }

  public static void main(String[] args) throws BindException, InjectionException, IOException {
    final Configuration clConfig = parseCommandLine(args);
    final Configuration fileConfig = parseConfigFile();
    runInMemory(clConfig, fileConfig);
    LOG.log(Level.INFO, "Job Submitted");
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private Launch() {
  }
}
