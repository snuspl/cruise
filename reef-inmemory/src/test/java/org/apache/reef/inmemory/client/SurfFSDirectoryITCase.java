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
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test Surf's directory-related operations
 */
public class SurfFSDirectoryITCase {
  private static final Logger LOG = Logger.getLogger(SurfFSDirectoryITCase.class.getName());

  private static SurfFS surfFs;

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String NONEMPTY = TESTDIR+"/"+"MKDIRS_nonempty";
  private static final String LEVEL1 = TESTDIR+"/"+"MKDIRS_level1";
  private static final String LEVEL2 = LEVEL1+"/"+"MKDIRS_level2";
  private static final String LEVEL3 = LEVEL2+"/"+"MKDIRS_level3";

  private static final String BEFORE = TESTDIR+"/"+"MKDIRS_before";
  private static final String AFTER = TESTDIR+"/"+"MKDIRS_after";

  private static final String DELETE = TESTDIR+"/"+"MKDIRS_delete";

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
  public static void tearDownClass() {
    // surfFs.delete(new Path(TESTDIR), true); TODO: Enable when delete is implemented
    surfLauncher.close();
  }

  /**
   * Test Filestatus of files in a directory
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
   * Test FileStatus of nested directories
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

  /**
   * Test file/directory renaming
   */
  @Test
  public void testRename() throws IOException  {
    assertEquals(true, surfFs.mkdirs(new Path(BEFORE)));
    for (int i = 0; i < 5; i ++) {
      final FSDataOutputStream outputStream = surfFs.create(new Path(BEFORE, String.valueOf(i)));
      outputStream.write((byte)1);
      outputStream.close();
    }

    // rename the directory and the children files
    assertEquals(true, surfFs.rename(new Path(BEFORE), new Path(AFTER)));
    for (int i = 0; i < 5; i ++) {
      assertEquals(true, surfFs.rename(new Path(AFTER, String.valueOf(i)), new Path(AFTER, String.valueOf(i+5))));
    }

    assertEquals(false, surfFs.exists(new Path(BEFORE)));
    assertEquals(true, surfFs.exists(new Path(AFTER)));
    for (int i = 0; i < 5; i ++) {
      assertEquals(false, surfFs.exists(new Path(BEFORE, String.valueOf(i))));
      assertEquals(false, surfFs.exists(new Path(AFTER, String.valueOf(i))));
      assertEquals(true, surfFs.exists(new Path(AFTER, String.valueOf(i+5))));
    }
  }

  /**
   * Test file/directory deletion
   */
  @Test
  public void testDelete() throws IOException {
    assertEquals(true, surfFs.mkdirs(new Path(DELETE)));
    for (int i = 0; i < 5; i ++) {
      final FSDataOutputStream outputStream = surfFs.create(new Path(DELETE, String.valueOf(i)));
      outputStream.write((byte)1);
      outputStream.close();
    }

    // delete a child file
    assertEquals(true, surfFs.delete(new Path(DELETE, String.valueOf(0)), true));
    assertEquals(false, surfFs.exists(new Path(DELETE, String.valueOf(0))));

    // delete the directory
    assertEquals(true, surfFs.delete(new Path(DELETE), true));
    assertEquals(false, surfFs.exists(new Path(DELETE)));
    for (int i = 0; i < 5; i ++) {
      assertEquals(false, surfFs.exists(new Path(DELETE, String.valueOf(i))));
    }
  }
}
