package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.*;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

public class CachedFSTest {

  private static final String HDFS = "hdfs";
  private static final String HDFS_ADDRESS = "localhost:9000";

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:9001";

  CachedFS cachedFs;

  // TODO: make this test independent of actually running HDFS (MiniCluster?)

  @Before
  public void setUp() throws IOException {
    cachedFs = new CachedFS();
    cachedFs.initialize(URI.create(SURF+"://"+SURF_ADDRESS), new Configuration());
  }

  @Test
  public void testFullPathListStatus() throws IOException {
    String absPath = "/user/bcho/README.md";
    FileStatus[] statuses = cachedFs.listStatus(new Path(SURF, SURF_ADDRESS, absPath));
    for (FileStatus status : statuses) {
      URI uri = status.getPath().toUri();
      assertEquals(SURF, uri.getScheme());
      assertEquals(SURF_ADDRESS, uri.getAuthority());
      assertEquals(absPath, uri.getPath());
    }
  }

  @Test
  public void testAbsPathListStatus() throws IOException {
    String absPath = "/user/bcho/README.md";
    FileStatus[] statuses = cachedFs.listStatus(new Path(absPath));
    for (FileStatus status : statuses) {
      URI uri = status.getPath().toUri();
      assertEquals(SURF, uri.getScheme());
      assertEquals(SURF_ADDRESS, uri.getAuthority());
      assertEquals(absPath, uri.getPath());
    }
  }

  @Test
  public void testHdfsPathToSurf() {
    String absPath = "/path/to/file";

    Path path = new Path(HDFS, HDFS_ADDRESS, absPath);
    assertTrue(path.isAbsolute());
    Path surfPath = cachedFs.pathToSurf(path);
    assertTrue(surfPath.isAbsolute());
    URI surfUri = surfPath.toUri();
    assertEquals(SURF, surfUri.getScheme());
    assertEquals(SURF_ADDRESS, surfUri.getAuthority());
    assertEquals(absPath, surfUri.getPath());
  }
}
