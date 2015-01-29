package org.apache.reef.inmemory.client.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.client.SurfFS;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.SurfLauncher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test CLI utility methods
 */
public final class CLIUtilsITCase {

  private static FileSystem baseFs;
  private static SurfFS surfFs;
  private static List<String> allPaths;

  private final static String TESTDIR = ITUtils.getTestDir();
  private final static String[] dirPaths = new String[] {TESTDIR+"/1", TESTDIR+"/1/2", TESTDIR+"/1/2/3"};
  private final static String[] filePaths = new String[] {"A", "B", "C"};

  private static final SurfLauncher surfLauncher = new SurfLauncher();

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   * Don't run destructive tests on the elements created here.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    Configuration hdfsConfig = new HdfsConfiguration();
    baseFs = ITUtils.getHdfs(hdfsConfig);

    surfLauncher.launch(baseFs);

    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    surfFs = new SurfFS();
    surfFs.initialize(URI.create("surf://localhost:18000"), conf);

    surfFs.mkdirs(new Path(dirPaths[2]));
    allPaths = new ArrayList<>(dirPaths.length * filePaths.length);
    for (String dir : dirPaths) {
      for (String file : filePaths) {
        final Path path = new Path(dir + "/" + file);
        final FSDataOutputStream stream = surfFs.create(path);
        stream.write((byte)1);
        stream.close();
        allPaths.add(path.toUri().getPath());
      }
    }
  }

  /**
   * Remove all directories.
   */
  @AfterClass
  public static void tearDownClass() throws IOException {
    for (final String dirPath : dirPaths) {
      surfFs.delete(new Path(dirPath), true);
    }
    surfLauncher.close();
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
