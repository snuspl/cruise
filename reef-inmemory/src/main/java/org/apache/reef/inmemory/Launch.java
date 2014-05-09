package org.apache.reef.inmemory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
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
  public static final Configuration getClientConfiguration(final Configuration runtimeConf) throws BindException {
    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, InMemoryClient.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, InMemoryClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, InMemoryClient.FailedJobHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(runtimeConf, clientConfiguration).build();
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
    final Configuration clientConf = getClientConfiguration(runtimeConf);

    LauncherStatus status = runInMemory(runtimeConf, clientConf);
    LOG.log(Level.INFO, "InMemory job completed: {0}", status);
  }

}