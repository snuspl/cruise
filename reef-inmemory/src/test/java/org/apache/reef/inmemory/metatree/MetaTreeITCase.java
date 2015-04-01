package org.apache.reef.inmemory.metatree;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.instrumentation.NullEventRecorder;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.driver.*;
import org.apache.reef.inmemory.driver.hdfs.*;
import org.apache.reef.inmemory.driver.metatree.MetaTree;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaTreeITCase {
  private static final int blockSize = 512;
  private static final String TESTDIR = ITUtils.getTestDir();

  private FileSystem hdfsClient;
  private MetaTree metaTree;

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   */
  @Before
  public void setUp() throws IOException {
    final CacheNodeManager manager = TestUtils.cacheManager();
    final ReplicationPolicy replicationPolicy = mock(ReplicationPolicy.class);

    for (int i = 0; i < 3; i++) {
      RunningTask task = TestUtils.mockRunningTask("" + i, "host" + i);
      manager.addRunningTask(task);
      manager.handleHeartbeat(task.getId(), TestUtils.cacheStatusMessage(18001 + i));
    }
    List<CacheNode> selectedNodes = manager.getCaches();
    assertEquals(3, selectedNodes.size());
    when(replicationPolicy.getReplicationAction(anyString(), any(FileMeta.class))).thenReturn(new Action(3, false));

    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);

    hdfsClient = ITUtils.getHdfs(hdfsConfig);
    hdfsClient.mkdirs(new Path(TESTDIR));

    final FileSystem baseFs = new BaseFsConstructor(ITUtils.getBaseFsAddress()).newInstance();
    metaTree = new MetaTree(new HDFSClient(baseFs), new NullEventRecorder());
  }

  /**
   * Remove all directories.
   */
  @After
  public void tearDown() throws IOException {
    hdfsClient.delete(new Path(TESTDIR), true);
  }

  /**
   * Test load of a non-existing path correctly throws FileNotFoundException
   */
  @Test(expected = FileNotFoundException.class)
  public void testLoadNonexistingPath() throws IOException {
    metaTree.getOrLoadFileMeta("/nonexistent/path");
  }

  /**
   * Test load of a directory (not a file) correctly throws FileNotFoundException
   * @throws IOException
   */
  @Test(expected = FileNotFoundException.class)
  public void testLoadDirectory() throws IOException {
    final String directory = TESTDIR+"/directory";

    hdfsClient.mkdirs(new Path(directory));
    final FileMeta fileMeta = metaTree.getOrLoadFileMeta(directory);
  }

  /**
   * Test proper loading of a small file. Checks that metadata is returned,
   * and correct.
   * The locations will be updated in {@link org.apache.reef.inmemory.driver.FileMetaUpdater#update}
   * @throws IOException
   */
  @Test
  public void testLoadSmallFile() throws IOException {
    final String smallFile = TESTDIR + "/smallFile";

    final FSDataOutputStream outputStream = hdfsClient.create(new Path(smallFile));
    outputStream.write(1);
    outputStream.close();

    final FileMeta fileMeta = metaTree.getOrLoadFileMeta(smallFile);
    assertNotNull(fileMeta);
    assertEquals(blockSize, fileMeta.getBlockSize());
    assertEquals(1, fileMeta.getFileSize());
  }


  /**
   * Test proper loading of a large file that spans multiple blocks.
   * Checks that metadata is returned, and correct.
   * The locations will be updated in {@link org.apache.reef.inmemory.driver.FileMetaUpdater#update}
   * In addition to the small file checks, the order of blocks is checked.
   * @throws IOException
   */
  @Test
  public void testLoadMultiblockFile() throws IOException {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final Path largeFile = ITUtils.writeFile(hdfsClient, TESTDIR + "/largeFile", chunkLength, numChunks);

    final FileMeta fileMeta = metaTree.getOrLoadFileMeta(largeFile.toString());
    assertNotNull(fileMeta);
    assertEquals(blockSize, fileMeta.getBlockSize());
    assertEquals(chunkLength * numChunks, fileMeta.getFileSize());
  }
}
