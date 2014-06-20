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
 * Tests for CachedFS, connecting to a base HDFS minicluster
 */
public class SurfFSTest {

  private static MiniDFSCluster cluster;
  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static final String TESTDIR = "/user/junit";
  private static final String TESTFILE = "README.md";
  private static final String ABSPATH = TESTDIR+"/"+TESTFILE;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:9001";

  // TODO: test relative paths

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
   * Test translation of base HDFS path to a Surf path
   */
  @Test
  public void testHdfsPathToSurf() {
    String absPath = "/path/to/file";

    Path path = new Path(baseFs.getUri().toString(), absPath);
    assertTrue(path.isAbsolute());
    Path surfPath = surfFs.pathToSurf(path);
    assertTrue(surfPath.isAbsolute());
    URI surfUri = surfPath.toUri();
    assertEquals(SURF, surfUri.getScheme());
    assertEquals(SURF_ADDRESS, surfUri.getAuthority());
    assertEquals(absPath, surfUri.getPath());
  }
}
