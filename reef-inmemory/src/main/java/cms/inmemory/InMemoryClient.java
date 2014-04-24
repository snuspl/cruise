package cms.inmemory;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationModule;

/**
 * Hello world!
 *
 */
public class InMemoryClient
{
  /**
   * Logger Object for System Log.
   */
  private static final Logger LOG = Logger.getLogger(InMemoryClient.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 1000000; // 10 sec.

  /**
   * Build a driver configuration and run InMemory application
   * 
   */
  public static LauncherStatus runInMemory(final Configuration runtimeConf, final int timeOut)
      throws BindException, InjectionException {

    ConfigurationModule driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "InMemory")
        .set(DriverConfiguration.ON_DRIVER_STARTED, InMemoryDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, InMemoryDriver.EvaluatorAllocatedHandler.class);//TANG 구현 
    driverConf = EnvironmentUtils.addClasspath(driverConf, DriverConfiguration.GLOBAL_LIBRARIES);

    return DriverLauncher.getLauncher(runtimeConf).run(driverConf.build(), timeOut);
  }

}
