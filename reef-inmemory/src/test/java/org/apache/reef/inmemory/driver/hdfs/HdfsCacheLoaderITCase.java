package org.apache.reef.inmemory.driver.hdfs;

import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.common.replication.SyncMethod;
import org.apache.reef.inmemory.common.replication.Write;
import org.apache.reef.inmemory.driver.CacheManagerImpl;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.TestUtils;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Tests for HdfsCacheLoader. All Hdfs operations are performed on a live
 * Hadoop minicluster.
 */
public final class HdfsCacheLoaderITCase {

  private static final int blockSize = 512;

  private FileSystem fs;
  private CacheManagerImpl manager;
  private HdfsCacheMessenger messenger;
  private HdfsCacheLoader loader;
  private HdfsCacheSelectionPolicy selector;
  private HdfsBlockIdFactory blockFactory;
  private ReplicationPolicy replicationPolicy;

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   */
  @Before
  public void setUp() throws IOException {
    manager = new CacheManagerImpl(mock(EvaluatorRequestor.class), "test", 0, 0, 0, 0, 0);
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
    when(replicationPolicy.getReplicationAction(anyString(), any(FileMeta.class))).thenReturn(new Action(3, false, new Write(SyncMethod.WRITE_BACK, 3)));

    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);

    fs = ITUtils.getHdfs(hdfsConfig);

    loader = new HdfsCacheLoader(manager, messenger, selector, blockFactory, replicationPolicy, fs.getUri().toString());
  }

  /**
   * Remove all directories.
   */
  @After
  public void tearDown() throws IOException {
    fs.delete(new Path("/*"), true);
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
    final Path smallFile = new Path("/existing/smallFile");

    final FSDataOutputStream outputStream = fs.create(smallFile);
    outputStream.write(1);
    outputStream.close();

    final FileMeta fileMeta = loader.load(smallFile);
    assertNotNull(fileMeta);
    assertNotNull(fileMeta.getBlocks());
    assertEquals(3, fileMeta.getBlocksIterator().next().getLocationsSize());
    assertEquals(blockSize, fileMeta.getBlockSize());
    assertEquals(smallFile.toString(), fileMeta.getFullPath());
  }

  /**
   * Test proper loading of a large file that spans multiple blocks.
   * Checks that metadata is returned, and correct.
   * In addition to the small file checks, the order of blocks is checked.
   * @throws IOException
   */
  @Test
  public void testLoadMultiblockFile() throws IOException {
    final Path largeFile = new Path("/existing/largeFile");

    final FSDataOutputStream outputStream = fs.create(largeFile);

    final int chunkLength = 2000;
    final int numChunks = 20;
    final byte[] writeChunk = new byte[chunkLength];
    for (int i = 0; i < numChunks; i++) {
      outputStream.write(writeChunk);
    }
    outputStream.close();

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem)fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength*numChunks);

    final FileMeta fileMeta = loader.load(largeFile);
    assertNotNull(fileMeta);
    assertNotNull(fileMeta.getBlocks());
    assertEquals(blockSize, fileMeta.getBlockSize());
    assertEquals(largeFile.toString(), fileMeta.getFullPath());

    final List<BlockInfo> blocks = fileMeta.getBlocks();
    assertEquals(locatedBlocks.getLocatedBlocks().size(), blocks.size());
    final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
            ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
    assertEquals(numBlocksComputed, blocks.size());
    for (int i = 0; i < blocks.size(); i++) {
      assertEquals(locatedBlocks.get(i).getBlock().getBlockId(), blocks.get(i).getBlockId());
      assertEquals(3, blocks.get(i).getLocationsSize());
    }
  }
}
