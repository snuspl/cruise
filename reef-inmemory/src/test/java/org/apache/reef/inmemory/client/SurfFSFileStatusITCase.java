package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.SurfLauncher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests SurfFS methods that query FileStatus.
 */
public final class SurfFSFileStatusITCase {
  private static final Logger LOG = Logger.getLogger(SurfFSFileStatusITCase.class.getName());

  private static FileSystem baseFs;
  private static SurfFS surfFs;
  private static final byte[] b = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};

  private static final String ROOTDIR = "/";
  private static final String TESTFILE = "README.md";
  private static final String ABSPATH = ROOTDIR +TESTFILE;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  private static final SurfLauncher surfLauncher = new SurfLauncher();

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   * Don't run destructive tests on the elements created here.
   */
  @BeforeClass
  public static void setUpClass() throws IOException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);

    baseFs = ITUtils.getHdfs(hdfsConfig);
    surfLauncher.launch(baseFs);
    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);

    final FSDataOutputStream stream = surfFs.create(new Path(ABSPATH));
    stream.write(b);
    stream.close();
  }

  /**
   * Clean up
   */
  @AfterClass
  public static void tearDownClass() {
    try {
      surfFs.delete(new Path(ROOTDIR), true);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to delete " + ROOTDIR, e);
    }
    surfLauncher.close();
  }

  /**
   * Test ls using a path including surf:// scheme and address
   */
  @Test
  public void testFullPathListStatus() throws IOException {
    testListStatusOfFile(new Path(SURF, SURF_ADDRESS, ABSPATH));
    testListStatusOfDir(new Path(SURF, SURF_ADDRESS, ROOTDIR));
  }

  /**
   * Test ls using an absolute path
   */
  @Test
  public void testAbsPathListStatus() throws IOException {
    testListStatusOfFile(new Path(ABSPATH));
    testListStatusOfDir(new Path(ROOTDIR));
  }

  /**
   * Test ls using a relative path
   */
  @Test
  public void testRelPathListStatus() throws IOException {
    // testListStatusOfFile(new Path(TESTFILE)); TODO
  }


  private void testListStatusOfDir(final Path dirPath) throws IOException {
    final FileStatus[] statuses = surfFs.listStatus(dirPath);
    assertEquals(1, statuses.length);
    final FileStatus fileStatusLS = statuses[0];
    testFileStatusOfTestFile(fileStatusLS);
  }

  private void testListStatusOfFile(final Path filePath) throws IOException {
    final FileStatus fileStatusGFS = surfFs.getFileStatus(filePath);
    final FileStatus[] statuses = surfFs.listStatus(filePath);
    assertEquals(1, statuses.length);
    final FileStatus fileStatusLS = statuses[0];

    assertEquals(fileStatusGFS.getPath(), fileStatusLS.getPath());
    testFileStatusOfTestFile(fileStatusGFS);
    testFileStatusOfTestFile(fileStatusLS);
  }

  private void testFileStatusOfTestFile(final FileStatus fileStatus) throws IOException {
    assertEquals(b.length, fileStatus.getLen());
    final URI uri = fileStatus.getPath().toUri();
    assertEquals(SURF, uri.getScheme());
    assertEquals(SURF_ADDRESS, uri.getAuthority());
    assertEquals(ABSPATH, uri.getPath());
  }

  /**
   * Test translation of base HDFS path to a Surf path
   */
  @Test
  public void testHdfsPathToSurf() {
    Path path = new Path(baseFs.getUri().toString(), ABSPATH);
    assertTrue(path.isAbsolute());
    Path surfPath = surfFs.toAbsoluteSurfPath(path);
    assertTrue(surfPath.isAbsolute());

    URI surfUri = surfPath.toUri();
    assertEquals(SURF, surfUri.getScheme());
    assertEquals(SURF_ADDRESS, surfUri.getAuthority());
    assertEquals(ABSPATH, surfUri.getPath());
  }

  /**
   * Test translation of Surf path to base HDFS path
   */
  @Test
  public void testSurfPathToHdfs() {
    Path path = new Path(surfFs.getUri().toString(), ABSPATH);
    assertTrue(path.isAbsolute());
    Path hdfsPath = surfFs.toAbsoluteBasePath(path);
    assertTrue(hdfsPath.isAbsolute());

    URI hdfsUri = hdfsPath.toUri();
    assertEquals(baseFs.getUri().getScheme(), hdfsUri.getScheme());
    assertEquals(baseFs.getUri().getAuthority(), hdfsUri.getAuthority());
    assertEquals(ABSPATH, hdfsUri.getPath());
  }
}
