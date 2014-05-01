package org.apache.reef.inmemory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
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
   * Build a driver configuration and run InMemory application
   */
  public static LauncherStatus run(final Configuration runtimeConf)
      throws BindException, InjectionException {

    ConfigurationModule driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "InMemory")
        .set(DriverConfiguration.ON_DRIVER_STARTED, InMemoryDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, InMemoryDriver.EvaluatorAllocatedHandler.class);

    driverConf = EnvironmentUtils.addClasspath(driverConf, DriverConfiguration.GLOBAL_LIBRARIES);
    return InMemoryClient.getClient(runtimeConf).run(driverConf.build());
  }

  /** 
   * run InMemory Application injecting InMemoryClient
   */
  public static void main(String[] args) throws BindException, InjectionException, IOException
  {
    final Configuration runtimeConf =  LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
        .build();
    
    LauncherStatus status = run(runtimeConf);
    LOG.log(Level.INFO, "InMemory job completed: {0}", status);
  }

}
