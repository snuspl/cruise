package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.SurfLauncher;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test creating files in Surf
 */
public final class SurfFSCreateITCase {
  private static final Logger LOG = Logger.getLogger(SurfFSCreateITCase.class.getName());

  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String TO_CREATE = TESTDIR + "/" + "CREATE_to_create";

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  private static final int DFS_REPLICATION_VALUE = 3;
  private static final int DFS_BLOCK_SIZE_VALUE = 512;

  private static final SurfLauncher surfLauncher = new SurfLauncher();

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   * Don't run destructive tests on the elements created here.
   */
  @BeforeClass
  public static void setUpClass() throws IOException, InjectionException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, DFS_REPLICATION_VALUE);
    // Reduce blocksize to 512 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_VALUE);

    baseFs = ITUtils.getHdfs(hdfsConfig);
    baseFs.mkdirs(new Path(TESTDIR));

    surfLauncher.launch(baseFs);

    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);
  }

  @AfterClass
  public static void tearDownClass() {
    try {
      surfFs.delete(new Path(TESTDIR), true);
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Failure occurred during cleanup", e);
    }
    surfLauncher.close();
  }

  /**
   * Test the file is created in Surf.
   * @throws IOException
   */
  @Test
  public void testCreate() throws IOException {
    final Path path = new Path(TO_CREATE);
    final FSDataOutputStream out = surfFs.create(path);
    out.close();

    // The file should be visible both in the Surf and the BaseFs.
    assertTrue(surfFs.exists(path));
    assertTrue(baseFs.exists(path));

    // Another create() fails because the file exists already.
    try {
      surfFs.create(path);
      fail("Should throw IOException because the file exists already");
    } catch (IOException e) {
      // passed
    } catch (Exception e) {
      fail("Should return IOException, instead returned " + e);
    }

    // Check the file can be opened.
    surfFs.open(path);
  }
}