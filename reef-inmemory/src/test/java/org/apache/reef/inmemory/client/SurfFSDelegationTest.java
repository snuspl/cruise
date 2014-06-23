package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

/**
 * Tests for SurfFS methods that delegate to a Base FS.
 * The tests use HDFS as the Base FS, by connecting to a base HDFS minicluster
 */
public final class SurfFSDelegationTest {

  private static MiniDFSCluster cluster;
  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static final String TESTDIR = "/user/"+System.getProperty("user.name");
  private static final String TESTFILE = "README.md";
  private static final String ABSPATH = TESTDIR+"/"+TESTFILE;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:9001";

  /**
   * Setup test environment once, since it's expensive.
   * Don't run destructive tests on the elements created here.
   */
  @BeforeClass
  public static void setUpClass() throws IOException {
    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);

    cluster = new MiniDFSCluster.Builder(hdfsConfig).numDataNodes(3).build();
    cluster.waitActive();

    baseFs = cluster.getFileSystem();
    baseFs.mkdirs(new Path(TESTDIR));

    FSDataOutputStream stream = baseFs.create(new Path(ABSPATH));
    stream.writeUTF("Hello Readme");
    stream.close();

    Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF+"://"+SURF_ADDRESS), conf);
  }

  /**
   * Test ls on directory
   */
  @Test
  public void testDirectoryListStatus() throws IOException {
    FileStatus[] statuses = surfFs.listStatus(new Path(SURF, SURF_ADDRESS, TESTDIR));
    for (FileStatus status : statuses) {
      URI uri = status.getPath().toUri();
      assertEquals(SURF, uri.getScheme());
      assertEquals(SURF_ADDRESS, uri.getAuthority());
      assertEquals(ABSPATH, uri.getPath());
    }
  }

  /**
   * Test ls using a path including surf:// scheme and address
   */
  @Test
  public void testFullPathListStatus() throws IOException {
    FileStatus[] statuses = surfFs.listStatus(new Path(SURF, SURF_ADDRESS, ABSPATH));
    for (FileStatus status : statuses) {
      URI uri = status.getPath().toUri();
      assertEquals(SURF, uri.getScheme());
      assertEquals(SURF_ADDRESS, uri.getAuthority());
      assertEquals(ABSPATH, uri.getPath());
    }
  }

  /**
   * Test ls using an absolute path
   */
  @Test
  public void testAbsPathListStatus() throws IOException {
    FileStatus[] statuses = surfFs.listStatus(new Path(ABSPATH));
    for (FileStatus status : statuses) {
      URI uri = status.getPath().toUri();
      assertEquals(SURF, uri.getScheme());
      assertEquals(SURF_ADDRESS, uri.getAuthority());
      assertEquals(ABSPATH, uri.getPath());
    }
  }

  /**
   * Test ls using a relative path
   */
  @Test
  public void testRelPathListStatus() throws IOException {
    FileStatus[] statuses = surfFs.listStatus(new Path(TESTFILE));
    for (FileStatus status : statuses) {
      URI uri = status.getPath().toUri();
      assertEquals(SURF, uri.getScheme());
      assertEquals(SURF_ADDRESS, uri.getAuthority());
      assertEquals(ABSPATH, uri.getPath());
    }
  }

  /**
   * Test translation of base HDFS path to a Surf path
   */
  @Test
  public void testHdfsPathToSurf() {
    Path path = new Path(baseFs.getUri().toString(), ABSPATH);
    assertTrue(path.isAbsolute());
    Path surfPath = surfFs.pathToSurf(path);
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
    Path hdfsPath = surfFs.pathToBase(path);
    assertTrue(hdfsPath.isAbsolute());

    URI hdfsUri = hdfsPath.toUri();
    assertEquals(baseFs.getUri().getScheme(), hdfsUri.getScheme());
    assertEquals(baseFs.getUri().getAuthority(), hdfsUri.getAuthority());
    assertEquals(ABSPATH, hdfsUri.getPath());
  }

  /**
   * Test set/get of working directory
   */
  @Test
  public void testWorkingDirectory() throws IOException {
    Path path = new Path("/user/otheruser");
    baseFs.mkdirs(path);

    Path original = surfFs.getWorkingDirectory();

    surfFs.setWorkingDirectory(path);
    assertEquals(path, surfFs.getWorkingDirectory());

    // Restore for other tests
    surfFs.setWorkingDirectory(original);
    assertEquals(original, surfFs.getWorkingDirectory());
  }
}
