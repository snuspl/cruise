package org.apache.reef.inmemory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
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
import com.microsoft.tang.formats.ConfigurationModule;

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
   * The number of iteration to run tasks
   */
  @NamedParameter(doc = "Number of times to run the command",
      short_name = "num_runs", default_value = "10")
  public static final class NumRuns implements Name<Integer> {
  }

  /**
   * Build a Runtime Configuration
   */ 
  public static final Configuration getRuntimeConfiguration() throws BindException {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
        .build();
  }

  /**
   * Build a Client Configuration
   */
  public static final Configuration getClientConfiguration(final Configuration runtimeConf, final Configuration commandLineConf) throws BindException {
    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, InMemoryClient.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, InMemoryClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, InMemoryClient.FailedJobHandler.class)
        /* TODO set more handlers
        .set(ClientConfiguration.ON_RUNTIME_ERROR, SurfClient.RuntimeErrorHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, SurfClient.JobMessageHandler.class)
         */
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(runtimeConf, clientConfiguration, commandLineConf).build();
  }

  /**
   * Build a configuration with the command line arguments
   */
  public static final Configuration getCommandLineConfiguration(String[] args) throws BindException {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(confBuilder);
      cl.registerShortNameOfClass(NumRuns.class);
      cl.processCommandLine(args);
    } catch (IOException e) {
      if(args.length != 2) 
        LOG.info("No argument received. Give a default value");
      LOG.severe("Failed to parse the command line");
    }
    return confBuilder.build();
  }

  /**
   * Build a driver configuration and run InMemory application
   */
  public static LauncherStatus runInMemory(final Configuration runtimeConf)
      throws BindException, InjectionException {

    ConfigurationModule driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "InMemory")
        .set(DriverConfiguration.ON_DRIVER_STARTED, InMemoryDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, InMemoryDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, InMemoryDriver.CompletedTaskHandler.class);
    driverConf = EnvironmentUtils.addClasspath(driverConf, DriverConfiguration.GLOBAL_LIBRARIES);

    return DriverLauncher.getLauncher(runtimeConf).run(driverConf.build());
  }

  /** 
   * run InMemory Application injecting InMemoryClient
   */
  public static LauncherStatus runInMemory(final Configuration runtimeConf, final Configuration clientConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(clientConf);
    final InMemoryClient client = injector.getInstance(InMemoryClient.class);
    client.submit();
    return client.waitForCompletion();
  }

  public static void main(String[] args) throws BindException, InjectionException, IOException
  {
    final Configuration runtimeConf = getRuntimeConfiguration();
    final Configuration cmdlineConf = getCommandLineConfiguration(args);

    final Configuration clientConf = getClientConfiguration(runtimeConf, cmdlineConf);

    LauncherStatus status = runInMemory(runtimeConf);
    // LauncherStatus status = runInMemory(runtimeConf, clientConf);
    LOG.log(Level.INFO, "InMemory job completed: {0}", status);
  }

}
