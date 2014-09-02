package org.apache.reef.inmemory.client.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.reef.inmemory.client.SurfFS;
import org.apache.reef.inmemory.common.service.SurfManagementService;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test CLI utility methods
 */
@Category(org.apache.reef.inmemory.common.IntensiveTests.class)
public final class CLIUtilsTest {

  private static MiniDFSCluster cluster;
  private static FileSystem baseFs;
  private static SurfFS surfFs;
  private static SurfManagementService.Client client;
  private static List<String> allPaths;

  private final static String[] dirPaths = new String[] {"/1", "/1/2", "/1/2/3"};
  private final static String[] filePaths = new String[] {"A", "B", "C"};

  /**
   * Setup base mini DFS cluster once, because it takes a long time.
   * Tests should not run any destructive operations on the base DFS.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    Configuration hdfsConfig = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(hdfsConfig).numDataNodes(1).build();
    cluster.waitActive();

    baseFs = cluster.getFileSystem();

    final String address = baseFs.getUri().getAuthority();
    client = CLI.getClient(address);

    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    surfFs = new SurfFS();
    surfFs.initialize(URI.create("surf://localhost:18000"), conf);

    baseFs.mkdirs(new Path(dirPaths[2]));
    allPaths = new ArrayList<>(dirPaths.length * filePaths.length);
    for (String dir : dirPaths) {
      for (String file : filePaths) {
        final Path path = new Path(dir + "/" + file);
        final FSDataOutputStream stream = baseFs.create(path);
        stream.write((byte)1);
        stream.close();
        allPaths.add(path.toUri().getPath());
      }
    }
  }

  /**
   * Test that returned recursive listing is correct when given a directory
   */
  @Test
  public void testRecursiveList() throws Exception {
    final List<FileStatus> fileList = CLIUtils.getRecursiveList(surfFs, dirPaths[0]);
    assertEquals(dirPaths.length * filePaths.length, fileList.size());
    for (FileStatus status : fileList) {
      assertTrue("Path equals "+status.getPath(), allPaths.contains(status.getPath().toUri().getPath()));
    }

    final List<FileStatus> levelOne = CLIUtils.getRecursiveList(surfFs, dirPaths[1]);
    assertEquals((dirPaths.length-1) * filePaths.length, levelOne.size());
    for (FileStatus status : levelOne) {
      assertTrue("Path equals "+status.getPath(), allPaths.contains(status.getPath().toUri().getPath()));
    }
  }

  /**
   * Test that returned recursive listing is correct when given a file: only the listing for the file should be returned
   */
  @Test
  public void testSingleFileList() throws Exception {
    final String file = dirPaths[0] + "/" + filePaths[0];
    final List<FileStatus> fileList = CLIUtils.getRecursiveList(surfFs, file);
    assertEquals(1, fileList.size());
    assertEquals(file, fileList.get(0).getPath().toUri().getPath());
  }

  /**
   * Test that FileNotFoundException is thrown for a path that does not exist
   */
  @Test(expected = FileNotFoundException.class)
  public void testNonExistentList() throws Exception {
    final List<FileStatus> fileList = CLIUtils.getRecursiveList(surfFs, "/nonexistent/path");
  }
}
