package org.apache.reef.inmemory.driver;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheLoader;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheMessenger;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheSelectionPolicy;
import org.apache.reef.inmemory.driver.hdfs.HdfsRandomCacheSelectionPolicy;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.junit.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test SurfMetaManager with the HdfsCacheLoader.
 * Tasks are mocked through mock CacheMessenger and CacheManager.
 * But other than the Tasks, the idea is to thoroughly test the SurfMetaManager, including its communication
 * with the HDFS NameNode.
 */
public final class SurfMetaManagerITCase {

  private static final int blockSize = 512;

  private FileSystem fs;
  private CacheManager manager;
  private HdfsCacheMessenger messenger;
  private CacheLoader<Path, FileMeta> loader;
  private LoadingCacheConstructor constructor;
  private LoadingCache<Path, FileMeta> cache;
  private HdfsCacheSelectionPolicy selector;
  private HdfsBlockIdFactory blockFactory;
  private ReplicationPolicy replicationPolicy;
  private SurfMetaManager metaManager;

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
    constructor = new LoadingCacheConstructor(loader);
    cache = constructor.newInstance();

    metaManager = new SurfMetaManager(cache, messenger);
  }

  /**
   * Remove all directories.
   */
  @After
  public void tearDown() throws IOException {
    fs.delete(new Path("/*"), true);
  }

  @Test
  public void testConcurrentLoadMultiblockFile() throws Throwable {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final String path = "/existing/largeFile";
    final Path largeFile = ITUtils.writeFile(fs, path, chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem)fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength*numChunks);

    final int numThreads =  10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);
    final Future<?>[] futures = new Future<?>[numThreads];
    final FileMeta[] fileMetas = new FileMeta[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      futures[index] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            fileMetas[index] = metaManager.getFile(new Path(path), new User());
          } catch (final Throwable t) {
            fail(t.toString());
            throw new RuntimeException(t);
          }
        }
      });
    }

    for (int i = 0; i < numThreads; i++) {
      futures[i].get();
    }

    // They should all return the exact same fileMeta
    for (int i = 0; i < numThreads - 1; i++) {
      for (int j = i+1; j < numThreads; j++) {
        assertTrue(fileMetas[i] == fileMetas[j]);
      }
    }

    // Sanity check
    final FileMeta fileMeta = fileMetas[0];
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
  public void testConcurrentReloadMultiblockFile() throws Throwable {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final String path = "/existing/largeFile";
    final Path largeFile = ITUtils.writeFile(fs, path, chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem)fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength*numChunks);

    final FileMeta fileMeta = metaManager.getFile(new Path(path), new User());

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

    final int numThreads =  10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);
    final Future<?>[] futures = new Future<?>[numThreads];
    final FileMeta[] fileMetas = new FileMeta[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      futures[index] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            fileMetas[index] = metaManager.getFile(new Path(path), new User());
          } catch (final Throwable t) {
            fail(t.toString());
            throw new RuntimeException(t);
          }
        }
      });
    }

    for (int i = 0; i < numThreads; i++) {
      futures[i].get();
    }

    // They should all return a fileMeta with at least locations > 0
    for (int index = 0; index < numThreads - 1; index++) {
      // Sanity check
      final FileMeta reloadedFileMeta = fileMetas[index];

      final List<BlockInfo> reloadedBlocks = reloadedFileMeta.getBlocks();
      assertEquals(locatedBlocks.getLocatedBlocks().size(), reloadedBlocks.size());
      final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
              ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
      assertEquals(numBlocksComputed, reloadedBlocks.size());
      for (int i = 0; i < reloadedBlocks.size(); i++) {
        assertEquals(locatedBlocks.get(i).getBlock().getBlockId(), reloadedBlocks.get(i).getBlockId());
        assertTrue(reloadedBlocks.get(i).getLocationsSize() > 0);
        assertTrue(3 >= reloadedBlocks.get(i).getLocationsSize());
      }
    }

    // The assumption is that the async reload will finish in 5 seconds, so the number of locations should equal three
    Thread.sleep(5000);
    {
      // Get the fileMeta directly from the cache, to avoid reloading again
      final FileMeta reloadedFileMeta = cache.getIfPresent(new Path(path));

      final List<BlockInfo> reloadedBlocks = reloadedFileMeta.getBlocks();
      assertEquals(locatedBlocks.getLocatedBlocks().size(), reloadedBlocks.size());
      final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
              ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
      assertEquals(numBlocksComputed, reloadedBlocks.size());
      for (int i = 0; i < reloadedBlocks.size(); i++) {
        assertEquals(locatedBlocks.get(i).getBlock().getBlockId(), reloadedBlocks.get(i).getBlockId());
        assertEquals(3, reloadedBlocks.get(i).getLocationsSize());
      }
    }
  }

  @Test
  public void testConcurrentRemovalReloadMultiblockFile() throws Throwable {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final String path = "/existing/largeFile";
    final Path largeFile = ITUtils.writeFile(fs, path, chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem)fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength*numChunks);

    final FileMeta fileMeta = metaManager.getFile(new Path(path), new User());
  }
}
