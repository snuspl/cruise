package org.apache.reef.inmemory.common;

import org.apache.hadoop.fs.FileSystem;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.inmemory.Launch;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launches and closes Surf via REEF DriverLauncher interface
 */
public class SurfLauncher {
  private static final Logger LOG = Logger.getLogger(SurfLauncher.class.getName());

  /**
   * The total execution time of Surf. The test must wait for this timeout in order to exit gracefully
   * (without leaving behind orphan processes). When adding more test cases,
   * you may need to increase this value.
   */
  private static final int SURF_TIMEOUT = 40 * 1000;

  /**
   * The time to wait for Surf graceful shutdown. If this time expires,
   * the user will have to hunt down orphan processes.
   */
  private static final int SURF_SHUTDOWN_WAIT = 40 * 1000;

  /**
   * The time to wait for Surf to complete startup. If Surf startup time increases, you may need
   * to increase this value.
   */
  private static final int SURF_STARTUP_SLEEP = 15 * 1000;

  /**
   * The time to wait for Surf to close. If Surf close time increases, you may need
   * to increase this value.
   */
  private static final int SURF_CLOSE_SLEEP = 10 * 1000;

  private static final Object lock = new Object();
  private static final AtomicBoolean jobFinished = new AtomicBoolean(false);

  private DriverLauncher driverLauncher;

  public void launch(final FileSystem baseFs) {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          final org.apache.reef.tang.Configuration clConf = Launch.parseCommandLine(new String[]{"-dfs_address", baseFs.getUri().toString()});
          final org.apache.reef.tang.Configuration fileConf = Launch.parseConfigFile();
          final org.apache.reef.tang.Configuration runtimeConfig = Launch.getRuntimeConfiguration(clConf, fileConf);
          final org.apache.reef.tang.Configuration launchConfig = Launch.getLaunchConfiguration(clConf, fileConf);

          driverLauncher = DriverLauncher.getLauncher(runtimeConfig);
          driverLauncher.run(launchConfig, SURF_TIMEOUT);

          jobFinished.set(true);
          synchronized (lock) {
            lock.notifyAll();
          }
        } catch (Exception e) {
          throw new RuntimeException("Could not run Surf instance", e);
        }
      }
    });

    try {
      Thread.sleep(SURF_STARTUP_SLEEP); // Wait for Surf setup before continuing
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted during SURF_STARTUP_SLEEP", e);
    }
  }

  public void close() {
    driverLauncher.close();
    if (!jobFinished.get()) {
      LOG.log(Level.INFO, "Waiting for Surf job to complete...");
      synchronized (lock) {
        try {
          lock.wait(SURF_SHUTDOWN_WAIT);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted during SURF_SHUTDOWN_WAIT", e);
        }
      }
    }

    if (!jobFinished.get()) {
      LOG.log(Level.SEVERE, "Surf did not exit gracefully. Please check for orphan processes (e.g. using `ps`) and kill them!");
    }

    try {
      Thread.sleep(SURF_CLOSE_SLEEP); // Wait for Surf to close before continuing
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted during SURF_CLOSE_SLEEP", e);
    }
  }
}
