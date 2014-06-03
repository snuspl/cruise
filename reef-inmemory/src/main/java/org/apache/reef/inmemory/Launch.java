package org.apache.reef.inmemory;

import java.util.logging.Level;
import java.util.logging.Logger;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.runtime.common.client.REEFImplementation;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

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


  public static void main(String[] args) throws BindException, InjectionException {
    final Configuration runtimeConfig = getRuntimeConfiguration();
    runInMemory(runtimeConfig);
    LOG.log(Level.INFO, "Job Submitted");
  }

}