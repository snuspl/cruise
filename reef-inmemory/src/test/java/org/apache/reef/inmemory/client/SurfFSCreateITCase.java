package org.apache.reef.inmemory.client;

import org.apache.reef.client.DriverLauncher;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.Launch;
import org.apache.reef.inmemory.common.ITUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.fail;

public class SurfFSCreateITCase {
  private static final Logger LOG = Logger.getLogger(SurfFSCreateITCase.class.getName());

  private static SurfFS surfFs;
  private static ExecutorService executorService = Executors.newSingleThreadExecutor();

  /**
   * The total execution time of Surf. The test must wait for this timeout in order to exit gracefully
   * (without leaving behind orphan processes). When adding more test cases,
   * you may need to increase this value.
   */
  private static final int SURF_TIMEOUT = 40 * 1000;

  /**
   * The time to wait for Surf to complete startup. If Surf startup time increases, you may need
   * to increase this value.
   */
  private static final int SURF_STARTUP_SLEEP = 15 * 1000;

  /**
   * The time to wait for Surf graceful shutdown. If this time expires,
   * the user will have to hunt down orphan processes.
   */
  private static final int SURF_SHUTDOWN_WAIT = 40 * 1000;

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String UNCLOSED = TESTDIR+"/"+"CREATE.unclosed";
  private static final String CLOSED = TESTDIR+"/"+"CREATE.closed";

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  private static final int DFS_REPLICATION_VALUE = 3;
  private static final int DFS_BLOCK_SIZE_VALUE = 512;

  private static final Object lock = new Object();
  private static final AtomicBoolean jobFinished = new AtomicBoolean(false);

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   * Don't run destructive tests on the elements created here.
   */
  @BeforeClass
  public static void setUpClass() throws IOException, InjectionException, InterruptedException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, DFS_REPLICATION_VALUE);
    // Reduce blocksize to 512 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_VALUE);

    final FileSystem baseFs = ITUtils.getHdfs(hdfsConfig);
    baseFs.mkdirs(new Path(TESTDIR));

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          final org.apache.reef.tang.Configuration clConf = Launch.parseCommandLine(new String[]{"-dfs_address", baseFs.getUri().toString()});
          final org.apache.reef.tang.Configuration fileConf = Launch.parseConfigFile();
          final org.apache.reef.tang.Configuration runtimeConfig = Launch.getRuntimeConfiguration(clConf, fileConf);
          final org.apache.reef.tang.Configuration launchConfig = Launch.getLaunchConfiguration(clConf, fileConf);

          DriverLauncher.getLauncher(runtimeConfig).run(launchConfig, SURF_TIMEOUT);
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
      e.printStackTrace();
    }

    final Configuration conf = new Configuration();
    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (!jobFinished.get()) {
      LOG.log(Level.INFO, "Waiting for Surf job to complete...");
      synchronized (lock) {
        lock.wait(SURF_SHUTDOWN_WAIT);
      }
    }

    if (!jobFinished.get()) {
      LOG.log(Level.SEVERE, "Surf did not exit gracefully. Please check for orphan processes (e.g. using `ps`) and kill them!");
    }
  }

  @Test
  public void testOutputStreamNotClosed() throws IOException {
    surfFs.create(new Path(UNCLOSED));

    try {
      surfFs.create(new Path(UNCLOSED));
      fail("Should return IOException. Because the file exists");
    } catch (IOException e) {
      // passed
    } catch (Exception e) {
      fail("Should return IOException, instead returned "+e);
    }
  }

  @Test
  public void testOutputStreamClosed() throws IOException {
    final FSDataOutputStream out1 = surfFs.create(new Path(CLOSED));
    out1.close();

    // should fail
    try {
      surfFs.create(new Path(CLOSED));
      fail("Should return IOException. Because the file exists");
    } catch (IOException e) {
      // passed
    } catch (Exception e) {
      fail("Should return IOException, instead returned "+e);
    }

    // should not fail
    surfFs.open(new Path(CLOSED));
  }
}