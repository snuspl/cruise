package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.util.SurfLauncher;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SurfFSMkdirsITCase {
  private static final Logger LOG = Logger.getLogger(SurfFSMkdirsITCase.class.getName());

  private static SurfFS surfFs;

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String NONEMPTY = TESTDIR+"/"+"MKDIRS_nonempty";
  private static final String LEVEL1 = TESTDIR+"/"+"MKDIRS_level1";
  private static final String LEVEL2 = LEVEL1+"/"+"MKDIRS_level2";
  private static final String LEVEL3 = LEVEL2+"/"+"MKDIRS_level3";

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

    final FileSystem baseFs = ITUtils.getHdfs(hdfsConfig);
    baseFs.mkdirs(new Path(TESTDIR));

    surfLauncher.launch(baseFs);

    final Configuration conf = new Configuration();
    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    // surfFs.delete(new Path(TESTDIR), true); TODO: Enable when delete is implemented
    surfLauncher.close();
  }

  /**
   * Write files in a directory and check their FileStatus
   */
  @Test
  public void testNonemptyDirectory() throws IOException {
    if (!surfFs.mkdirs(new Path(NONEMPTY))) {
      fail("There should be no existing directory structure");
    }

    for (int i = 0; i < 5; i++) {
      final FSDataOutputStream outputStream = surfFs.create(new Path(NONEMPTY + "/" + String.valueOf(i)));
      for (int j = 0; j < i; j++) {
        outputStream.write((byte)j);
      }
      outputStream.close();
    }

    final FileStatus[] fileStatuses = surfFs.listStatus(new Path(NONEMPTY));
    for (int i = 0; i < 5; i++) {
      final FileStatus fileStatus = fileStatuses[i];
      assertEquals(NONEMPTY + "/" + String.valueOf(i), fileStatus.getPath().toString());
      assertEquals(i, fileStatus.getLen());
      // TODO: Test other attributes in FileStatus
    }
  }

  /**
   * Created nested directories and check their FileStatus
   */
  @Test
  public void testNestedDirectory() throws IOException {
    if (!surfFs.mkdirs(new Path(LEVEL1))) {
      fail("There should be no existing directory structure");
    }
    if (!surfFs.mkdirs(new Path(LEVEL2))) {
      fail("There should be no existing directory structure");
    }
    if (!surfFs.mkdirs(new Path(LEVEL3))) {
      fail("There should be no existing directory structure");
    }

    for (FileStatus fileStatus : surfFs.listStatus(new Path(LEVEL1))) {
      assertEquals(LEVEL2, fileStatus.getPath().toString());
      assertEquals(true, fileStatus.isDirectory());
      // TODO: Test other attributes in FileStatus
    }
    for (FileStatus fileStatus : surfFs.listStatus(new Path(LEVEL2))) {
      assertEquals(LEVEL3, fileStatus.getPath().toString());
      assertEquals(true, fileStatus.isDirectory());
      // TODO: Test other attributes in FileStatus
    }
    final FileStatus[] fileStatuses = surfFs.listStatus(new Path(LEVEL3));
    if (fileStatuses.length != 0) {
      fail("There should be nothing in the directory");
    }
  }
}
