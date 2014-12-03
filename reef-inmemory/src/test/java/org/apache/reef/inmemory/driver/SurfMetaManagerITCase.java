package org.apache.reef.inmemory.driver;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.reef.driver.task.RunningTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.common.instrumentation.NullEventRecorder;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.common.replication.SyncMethod;
import org.apache.reef.inmemory.common.replication.Write;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheLoader;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheMessenger;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheSelectionPolicy;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheUpdater;
import org.apache.reef.inmemory.driver.hdfs.HdfsRandomCacheSelectionPolicy;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.driver.locality.YarnLocationSorter;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
  private static final String TESTDIR = ITUtils.getTestDir();

  private EventRecorder RECORD;
  private FileSystem fs;
  private CacheManager manager;
  private HdfsCacheMessenger messenger;
  private CacheLoader<Path, FileMeta> loader;
  private LoadingCacheConstructor constructor;
  private LoadingCache<Path, FileMeta> cache;
  private CacheUpdater cacheUpdater;
  private LocationSorter locationSorter;
  private HdfsCacheSelectionPolicy selector;
  private CacheLocationRemover cacheLocationRemover;
  private HdfsBlockIdFactory blockFactory;
  private ReplicationPolicy replicationPolicy;
  private SurfMetaManager metaManager;

  @Before
  public void setUp() throws IOException {
    RECORD = new NullEventRecorder();
    manager = TestUtils.cacheManager();
    messenger = new HdfsCacheMessenger(manager);
    selector = new HdfsRandomCacheSelectionPolicy();
    cacheLocationRemover = new CacheLocationRemover();
    blockFactory = new HdfsBlockIdFactory();
    replicationPolicy = mock(ReplicationPolicy.class);


    for (int i = 0; i < 3; i++) {
      final RunningTask task = TestUtils.mockRunningTask("" + i, "host" + i);

      manager.addRunningTask(task);
      manager.handleHeartbeat(task.getId(), TestUtils.cacheStatusMessage(18001+i));
    }
    List<CacheNode> selectedNodes = manager.getCaches();
    assertEquals(3, selectedNodes.size());
    when(replicationPolicy.getReplicationAction(anyString(), any(FileMeta.class))).thenReturn(new Action(3, false, new Write(SyncMethod.WRITE_BACK, 1)));

    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);

    fs = ITUtils.getHdfs(hdfsConfig);
    fs.mkdirs(new Path(TESTDIR));

    loader = new HdfsCacheLoader(
            manager, messenger, selector, blockFactory, replicationPolicy, fs.getUri().toString(), RECORD);
    constructor = new LoadingCacheConstructor(loader);
    cache = constructor.newInstance();

    cacheUpdater = new HdfsCacheUpdater(manager, messenger, selector, cacheLocationRemover, blockFactory, replicationPolicy, fs.getUri().toString());
    locationSorter = new YarnLocationSorter(new YarnConfiguration());

    metaManager = new SurfMetaManager(cache, messenger, cacheLocationRemover, cacheUpdater, blockFactory, locationSorter);
  }

  /**
   * Remove all directories.
   */
  @After
  public void tearDown() throws IOException {
    fs.delete(new Path(TESTDIR), true);
  }

  /**
   * Test concurrent gets on an unloaded file.
   * Each get should return with metadata of the newly loaded file.
   */
  @Test
  public void testConcurrentLoad() throws Throwable {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final String path = TESTDIR+"/largeFile";
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
            fileMetas[index] = metaManager.getFile(new Path(path), new User(), "host0");
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

    // They should all return different fileMetas
    for (int i = 0; i < numThreads - 1; i++) {
      for (int j = i+1; j < numThreads; j++) {
        assertFalse(fileMetas[i] == fileMetas[j]);
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

  /**
   * Test concurrent gets on a loaded file, that has missing blocks.
   * Each get should return with metdata that has loaded at least one location per block.
   */
  @Test
  public void testConcurrentUpdate() throws Throwable {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final String path = TESTDIR+"/largeFile";
    final Path largeFile = ITUtils.writeFile(fs, path, chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem)fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength*numChunks);

    final FileMeta fileMeta = metaManager.getFile(new Path(path), new User(), "host0");

    final List<BlockInfo> blocks = fileMeta.getBlocks();
    // Remove first location from first block
    {
      final BlockInfo block = blocks.get(0);
      final Iterator<NodeInfo> it = block.getLocationsIterator();
      final NodeInfo nodeInfo = it.next();
      cacheLocationRemover.remove(fileMeta.getFullPath(), blockFactory.newBlockId(block), nodeInfo.getAddress());
    }
    // Remove all locations from second block
    {
      final BlockInfo block = blocks.get(1);
      final Iterator<NodeInfo> it = block.getLocationsIterator();
      while (it.hasNext()) {
        final NodeInfo nodeInfo = it.next();
        cacheLocationRemover.remove(fileMeta.getFullPath(), blockFactory.newBlockId(block), nodeInfo.getAddress());
      }
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
            fileMetas[index] = metaManager.getFile(new Path(path), new User(), "host0");
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
    for (int index = 0; index < numThreads; index++) {
      // Sanity check
      final FileMeta updatedFileMeta = fileMetas[index];
      assertFalse(updatedFileMeta == fileMeta);

      final List<BlockInfo> updatedBlocks = updatedFileMeta.getBlocks();
      assertEquals(locatedBlocks.getLocatedBlocks().size(), updatedBlocks.size());
      final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
              ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
      assertEquals(numBlocksComputed, updatedBlocks.size());
      for (int i = 0; i < updatedBlocks.size(); i++) {
        assertEquals(locatedBlocks.get(i).getBlock().getBlockId(), updatedBlocks.get(i).getBlockId());
        assertTrue(updatedBlocks.get(i).getLocationsSize() > 0);
        assertTrue(3 >= updatedBlocks.get(i).getLocationsSize());
      }
    }
  }

  /**
   * Test concurrent gets with block removal updates.
   * Each get should return with metdata that has loaded at least one location per block.
   */
  @Test
  public void testConcurrentRemoveAndUpdate() throws Throwable {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final String path = TESTDIR+"/largeFile";
    final Path largeFile = ITUtils.writeFile(fs, path, chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem)fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength*numChunks);

    final FileMeta fileMeta = metaManager.getFile(new Path(path), new User(), "host0");

    final int numThreads =  20;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);
    final Future<?>[] futures = new Future<?>[numThreads];
    final FileMeta[] fileMetas = new FileMeta[numThreads];

    final List<BlockInfo> blocks = fileMeta.getBlocks();
    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      futures[index] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            if (index % 2 == 0) {
              fileMetas[index] = metaManager.getFile(new Path(path), new User(), "host0");
            } else {
              final BlockInfo block = blocks.get(index);
              final Iterator<NodeInfo> it = block.getLocationsIterator();
              while (it.hasNext()) {
                final NodeInfo nodeInfo = it.next();
                cacheLocationRemover.remove(fileMeta.getFullPath(), blockFactory.newBlockId(block), nodeInfo.getAddress());
              }
            }
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
    for (int index = 0; index < numThreads; index++) {
      if (index % 2 != 0) { // ignore the runs that did not get a FileMeta
        continue;
      }
      // Sanity check
      final FileMeta updatedFileMeta = fileMetas[index];
      assertFalse(updatedFileMeta == fileMeta);

      final List<BlockInfo> updatedBlocks = updatedFileMeta.getBlocks();
      assertEquals(locatedBlocks.getLocatedBlocks().size(), updatedBlocks.size());
      final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
              ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
      assertEquals(numBlocksComputed, updatedBlocks.size());
      for (int i = 0; i < updatedBlocks.size(); i++) {
        assertEquals(locatedBlocks.get(i).getBlock().getBlockId(), updatedBlocks.get(i).getBlockId());
        assertTrue(updatedBlocks.get(i).getLocationsSize() > 0);
        assertTrue(3 >= updatedBlocks.get(i).getLocationsSize());
      }
    }
  }
}
