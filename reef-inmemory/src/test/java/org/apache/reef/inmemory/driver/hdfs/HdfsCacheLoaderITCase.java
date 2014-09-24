package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.driver.CacheManagerImpl;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.TestUtils;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;
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
      manager.handleHeartbeat(task.getId(), TestUtils.cacheStatusMessage(18001+i));
    }
    List<CacheNode> selectedNodes = manager.getCaches();
    assertEquals(3, selectedNodes.size());
    when(replicationPolicy.getReplicationAction(anyString(), any(FileMeta.class))).thenReturn(new Action(3, false));

    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);

    fs = ITUtils.getHdfs(hdfsConfig);
    fs.mkdirs(new Path("/existing"));

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
    final int chunkLength = 2000;
    final int numChunks = 20;
    final Path largeFile = ITUtils.writeFile(fs, "/existing/largeFile", chunkLength, numChunks);

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

  @Test
  public void testReloadMultiblockFile() throws Exception {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final Path largeFile = ITUtils.writeFile(fs, "/existing/largeFile", chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem)fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength*numChunks);

    final FileMeta fileMeta = loader.load(largeFile);
    final ListenableFuture<FileMeta> reloadedFuture = loader.reload(largeFile, fileMeta);
    assertTrue(fileMeta == reloadedFuture.get());

    final List<BlockInfo> blocks = fileMeta.getBlocks();
    // Remove first location from first block
    {
      final BlockInfo block = blocks.get(0);
      final Iterator<NodeInfo> it = block.getLocationsIterator();
      it.next();
      it.remove();
      assertEquals(2, block.getLocationsSize());
    }
    // Remove all locations from second block
    {
      final BlockInfo block = blocks.get(1);
      final Iterator<NodeInfo> it = block.getLocationsIterator();
      while (it.hasNext()) {
        it.next();
        it.remove();
      }
      assertEquals(0, block.getLocationsSize());
    }
    // (Leave rest of the blocks alone)

    // Reload -- this should reload blocks to the missing locations
    final ListenableFuture<FileMeta> future = loader.reload(largeFile, fileMeta);
    // The following passes in practice, but it depends on a race:
    // that this line is called before the async part of reload finishes. Leaving it in for now.
    assertFalse(future.isDone());

    final FileMeta reloadedFileMeta = future.get();
    assertNotEquals(fileMeta, reloadedFileMeta); // Because we called future.get, will be a new

    final List<BlockInfo> reloadedBlocks = reloadedFileMeta.getBlocks();
    assertEquals(locatedBlocks.getLocatedBlocks().size(), reloadedBlocks.size());
    final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
            ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
    assertEquals(numBlocksComputed, reloadedBlocks.size());
    for (int i = 0; i < reloadedBlocks.size(); i++) {
      assertEquals(locatedBlocks.get(i).getBlock().getBlockId(), reloadedBlocks.get(i).getBlockId());
      assertEquals(3, reloadedBlocks.get(i).getLocationsSize());
    }

    // Reload -- with no locations missing, this should return with the same FileMeta
    final ListenableFuture<FileMeta> future2 = loader.reload(largeFile, reloadedFileMeta);
    assertTrue(future2.isDone());
    final FileMeta reloadedFileMeta2 = future2.get();
    assertTrue(reloadedFileMeta == reloadedFileMeta2);
  }
}
