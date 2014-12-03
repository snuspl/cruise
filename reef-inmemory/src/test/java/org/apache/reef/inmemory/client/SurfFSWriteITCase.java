package org.apache.reef.inmemory.client;

import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.REEF;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.Launch;
import org.apache.reef.inmemory.common.ITUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class SurfFSWriteITCase {

  private static final Logger LOG = Logger.getLogger(SurfFSWriteITCase.class.getName());

  private static FileSystem baseFs;
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

  private static final byte[] b = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};

  private static final String TESTDIR = "/user/"+System.getProperty("user.name");

  private static final String TESTPATH1 = TESTDIR+"/"+"WRITE.short";
  private static final int SIZE1 = 1;

  private static final String TESTPATH2 = TESTDIR+"/"+"WRITE.long";
  private static final int SIZE2 = 200;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  private static final int BLOCK_SIZE = 800;

  private static final Object lock = new Object();
  private static final AtomicBoolean jobFinished = new AtomicBoolean(false);

  @BeforeClass
  public static void setUpClass() throws IOException, InjectionException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Reduce blocksize to 800 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    baseFs = ITUtils.getHdfs(hdfsConfig);
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
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);

    final FSDataOutputStream stream1 = surfFs.create(new Path(TESTPATH1), true, 4096, (short)3, BLOCK_SIZE);
    for (int i = 0; i < SIZE1; i++) {
      stream1.write(b);
    }
    stream1.close();

    final FSDataOutputStream stream2 = surfFs.create(new Path(TESTPATH2), true, 4096, (short)3, BLOCK_SIZE);
    for (int i = 0; i < SIZE2; i++) {
      stream2.write(b);
    }
    stream2.close();
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

  private FSDataInputStream open(Path path) throws IOException {
    FSDataInputStream in = null;
    in = surfFs.open(path);
    return in;
  }

  private void read(String path, int size) throws IOException {
    FSDataInputStream in = open(new Path(path));

    byte[] readBuf = new byte[size * b.length];

    int bytesRead = in.read(0, readBuf, 0, readBuf.length);
    assertEquals(bytesRead, readBuf.length);
    for (int i = 0; i < size * b.length; i++) {
      assertEquals("At index "+i, b[i % b.length], readBuf[i]);
    }
  }

  @Test
  public void testRead() throws IOException {
    read(TESTPATH1, SIZE1);
    read(TESTPATH2, SIZE2);
  }

  @Test
  public void testExists() throws IOException {
    assertTrue(surfFs.exists(new Path(TESTPATH1)));
    assertTrue(surfFs.exists(new Path(TESTPATH2)));
    assertFalse(surfFs.exists(new Path("Not_exists")));
  }
}