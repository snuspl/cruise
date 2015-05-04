package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
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
import java.io.OutputStream;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test {@link org.apache.reef.inmemory.client.SurfFS#delete}
 */
public class SurfFSDeleteITCase {
  private static final Logger LOG = Logger.getLogger(SurfFSDeleteITCase.class.getName());

  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String SURF_FILE = TESTDIR + "/" + "DELETE.surf";
  private static final String BASE_FILE = TESTDIR + "/" + "DELETE.base";

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  private static final int DFS_REPLICATION_VALUE = 3;
  private static final int DFS_BLOCK_SIZE_VALUE = 512;

  private static final SurfLauncher surfLauncher = new SurfLauncher();

  private static final byte[] CHUNK = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};
  private static final int NUM_CHUNKS = 1024;

  private static final short REPLICATION = 3;
  private static final int BLOCK_SIZE = 1024; // Need to be a multiple of 512 (Hadoop Checksum Policy)
  private static final int BUFFER_SIZE = 4096;

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

    writeData(baseFs, BASE_FILE);

    surfLauncher.launch(baseFs);

    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);

    writeData(surfFs, SURF_FILE);
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
   * Test deleting a file that is loaded from BaseFs. The file should be removed from both
   * Surf and BaseFs.
   * @throws java.io.IOException
   */
  @Test
  public void testDeleteLoadedFile() throws IOException {
    final Path path = new Path(BASE_FILE);
    try {
      if (!surfFs.delete(path, false)) {
        LOG.log(Level.SEVERE, "The file {0} is not deleted!", BASE_FILE);
      }
    } catch (IOException e) {
      fail("Should delete the file successfully, instead returned " + e);
    }
    assertFalse(surfFs.exists(path));
    assertFalse(baseFs.exists(path));
  }


  /**
   * Test deleting a file that is created in Surf. The file should be removed from both
   * Surf and BaseFs.
   * @throws java.io.IOException
   */
  @Test
  public void testDeleteCreatedFile() throws IOException {
    final Path path = new Path(SURF_FILE);
    try {
      if (!surfFs.delete(path, false)) {
        LOG.log(Level.SEVERE, "The file {0} is not deleted!", SURF_FILE);
      }
    } catch (IOException e) {
      fail("Should delete the file successfully, instead returned " + e);
    }
    assertFalse(surfFs.exists(path));
    assertFalse(baseFs.exists(path));
  }

  /**
   * Write data to the file.
   * @param fileSystem the file system to write the file.
   * @param filePath the path of the file.
   * @throws IOException
   */
  private static void writeData(final FileSystem fileSystem, final String filePath) throws IOException {
    final OutputStream stream = fileSystem.create(new Path(filePath), true, BUFFER_SIZE, REPLICATION, BLOCK_SIZE);
    for (int i = 0; i < NUM_CHUNKS; i++) {
      stream.write(CHUNK);
    }
    stream.close();
  }
}
