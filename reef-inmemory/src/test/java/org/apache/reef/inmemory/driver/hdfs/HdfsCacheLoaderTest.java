package org.apache.reef.inmemory.driver.hdfs;

import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.driver.CacheManagerImpl;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.TestUtils;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for HdfsCacheLoader. All Hdfs operations are performed on a live
 * MiniDFSCluster.
 */
@Category(org.apache.reef.inmemory.common.IntensiveTests.class)
public final class HdfsCacheLoaderTest {

  private MiniDFSCluster cluster;
  private FileSystem fs;
  private CacheManagerImpl manager;
  private HdfsCacheMessenger messenger;
  private HdfsCacheLoader loader;
  private HdfsCacheSelectionPolicy selector;
  private HdfsBlockIdFactory blockFactory;
  private ReplicationPolicy replicationPolicy;

  @Before
  public void setUp() throws IOException {
    manager = new CacheManagerImpl(mock(EvaluatorRequestor.class), "test", 0, 0, 0, 0);
    messenger = new HdfsCacheMessenger(manager);
    selector = new HdfsRandomCacheSelectionPolicy();
    blockFactory = new HdfsBlockIdFactory();
    replicationPolicy = mock(ReplicationPolicy.class);

    for (int i = 0; i < 3; i++) {
      RunningTask task = TestUtils.mockRunningTask("" + i, "host" + i);

      manager.addRunningTask(task);
      manager.handleHeartbeat(task.getId(), TestUtils.cacheStatusMessage(18001));
    }
    List<CacheNode> selectedNodes = manager.getCaches();
    assertEquals(3, selectedNodes.size());
    when(replicationPolicy.getReplicationAction(anyString(), any(FileMeta.class))).thenReturn(new Action(3, false));

    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);

    cluster = new MiniDFSCluster.Builder(hdfsConfig).numDataNodes(3).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    loader = new HdfsCacheLoader(manager, messenger, selector, blockFactory, replicationPolicy, fs.getUri().toString());
  }

  @After
  public void tearDown() {
    cluster.shutdown();
  }

  /**
   * Test load of a non-existing path correctly throws FileNotFoundException
   */
  @Test(expected = FileNotFoundException.class)
  public void testLoadNonexistingPath() throws IOException {
    loader.load(new Path("/nonexistent/path"));
  }

  /**
   * Test load of a directory (not a file) correctly throws FileNotFoundException
   * @throws IOException
   */
  @Test(expected = FileNotFoundException.class)
  public void testLoadDirectory() throws IOException {
    final Path directory = new Path("/existing/directory");

    fs.mkdirs(directory);
    final FileMeta fileMeta = loader.load(directory);
  }

  /**
   * Test proper loading of a small file. Checks that metadata is returned,
   * and correct.
   * @throws IOException
   */
  @Test
  public void testLoadSmallFile() throws IOException {
    final Path smallFile = new Path("/existing/file");

    final FSDataOutputStream outputStream = fs.create(smallFile);
    outputStream.write(1);
    outputStream.close();

    final FileMeta fileMeta = loader.load(smallFile);
    assertNotNull(fileMeta);
    assertNotNull(fileMeta.getBlocks());
    assertEquals(3, fileMeta.getBlocksIterator().next().getLocationsSize());

  }
}
