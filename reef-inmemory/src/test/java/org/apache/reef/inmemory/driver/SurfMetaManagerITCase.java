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
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockInfoFactory;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMetaFactory;
import org.apache.reef.inmemory.common.hdfs.HdfsFileMetaFactory;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.common.instrumentation.NullEventRecorder;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.common.replication.SyncMethod;
import org.apache.reef.inmemory.common.replication.Write;
import org.apache.reef.inmemory.driver.hdfs.*;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.driver.locality.YarnLocationSorter;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
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
  private CacheNodeManager manager;
  private HdfsCacheNodeMessenger messenger;
  private CacheLoader<Path, FileMeta> loader;
  private LoadingCacheConstructor constructor;
  private LoadingCache<Path, FileMeta> cache;
  private FileMetaUpdater fileMetaUpdater;
  private LocationSorter locationSorter;
  private HdfsCacheSelectionPolicy selector;
  private CacheLocationRemover cacheLocationRemover;
  private HdfsBlockMetaFactory blockMetaFactory;
  private HdfsBlockInfoFactory blockInfoFactory;
  private HdfsFileMetaFactory fileMetaFactory;
  private HdfsBlockLocationGetter blockLocationGetter;
  private ReplicationPolicy replicationPolicy;
  private SurfMetaManager metaManager;
  private FileSystem baseFs;
  private HDFSClient baseFsClient;

  @Before
  public void setUp() throws IOException {
    RECORD = new NullEventRecorder();
    manager = TestUtils.cacheManager();
    messenger = new HdfsCacheNodeMessenger(manager);
    selector = new HdfsRandomCacheSelectionPolicy();
    cacheLocationRemover = new CacheLocationRemover();
    blockMetaFactory = new HdfsBlockMetaFactory();
    blockInfoFactory = new HdfsBlockInfoFactory();
    fileMetaFactory = new HdfsFileMetaFactory();
    replicationPolicy = mock(ReplicationPolicy.class);

    for (int i = 0; i < 3; i++) {
      final RunningTask task = TestUtils.mockRunningTask("" + i, "host" + i);

      manager.addRunningTask(task);
      manager.handleHeartbeat(task.getId(), TestUtils.cacheStatusMessage(18001 + i));
    }
    List<CacheNode> selectedNodes = manager.getCaches();
    assertEquals(3, selectedNodes.size());
    when(replicationPolicy.getReplicationAction(anyString(), any(FileMeta.class))).thenReturn(new Action(3, false, new Write(SyncMethod.WRITE_BACK, 1)));

    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);

    fs = ITUtils.getHdfs(hdfsConfig);
    fs.mkdirs(new Path(TESTDIR));

    baseFs = new BaseFsConstructor(ITUtils.getBaseFsAddress()).newInstance();
    blockLocationGetter = new HdfsBlockLocationGetter(baseFs);
    baseFsClient = new HDFSClient(baseFs, fileMetaFactory);

    loader = new HdfsMetaLoader(baseFs, fileMetaFactory, RECORD);
    constructor = new LoadingCacheConstructor(loader);
    cache = constructor.newInstance();

    fileMetaUpdater = new HdfsFileMetaUpdater(manager, messenger, selector, cacheLocationRemover, blockMetaFactory, blockInfoFactory, replicationPolicy, baseFs, blockLocationGetter);
    locationSorter = new YarnLocationSorter(new YarnConfiguration());

    metaManager = new SurfMetaManager(cache, messenger, cacheLocationRemover, fileMetaUpdater, blockMetaFactory, fileMetaFactory, locationSorter, baseFsClient);
  }

  /**
   * Remove all directories.
   */
  @After
  public void tearDown() throws IOException {
    fs.delete(new Path(TESTDIR), true);
    baseFs.close();
  }

  /**
   * Test concurrent gets on an unloaded file.
   * Each get should return with metadata of the newly loaded file.
   */
  @Test
  public void testConcurrentLoad() throws Throwable {
    final int chunkLength = 2000;
    final int numChunks = 20;
    final String path = TESTDIR + "/largeFile";
    final Path largeFile = ITUtils.writeFile(fs, path, chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem) fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength*numChunks);

    final int numThreads = 10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);
    final Future<?>[] futures = new Future<?>[numThreads];
    final FileMeta[] fileMetas = new FileMeta[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      futures[index] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            final FileMeta fileMeta = metaManager.get(new Path(path), new User());
            fileMetas[index] = metaManager.loadData(fileMeta);
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
      for (int j = i + 1; j < numThreads; j++) {
        assertFalse(fileMetas[i] == fileMetas[j]);
      }
    }

    // Sanity check
    final FileMeta fileMeta = fileMetas[0];
    assertNotNull(fileMeta);
    assertNotNull(fileMeta.getBlocks());
    assertEquals(blockSize, fileMeta.getBlockSize());
    assertEquals(largeFile.toString(), fileMeta.getFullPath());

    final List<BlockMeta> blocks = fileMeta.getBlocks();
    assertEquals(locatedBlocks.getLocatedBlocks().size(), blocks.size());
    final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
            ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
    assertEquals(numBlocksComputed, blocks.size());
    for (int i = 0; i < blocks.size(); i++) {
      assertEquals(locatedBlocks.get(i).getStartOffset(), blocks.get(i).getOffSet());
      assertEquals(locatedBlocks.get(i).getBlockSize(), blocks.get(i).getLength());
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
    final String path = TESTDIR + "/largeFile";
    final Path largeFile = ITUtils.writeFile(fs, path, chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem) fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength * numChunks);

    final FileMeta fm = metaManager.get(new Path(path), new User());
    final FileMeta fileMeta = metaManager.loadData(fm);

    final List<BlockMeta> blocks = fileMeta.getBlocks();
    // Remove first location from first block
    {
      final BlockMeta block = blocks.get(0);
      final Iterator<NodeInfo> it = block.getLocationsIterator();
      final NodeInfo nodeInfo = it.next();
      cacheLocationRemover.remove(fileMeta.getFullPath(), new BlockId(block), nodeInfo.getAddress());
    }
    // Remove all locations from second block
    {
      final BlockMeta block = blocks.get(1);
      final Iterator<NodeInfo> it = block.getLocationsIterator();
      while (it.hasNext()) {
        final NodeInfo nodeInfo = it.next();
        cacheLocationRemover.remove(fileMeta.getFullPath(), new BlockId(block), nodeInfo.getAddress());
      }
    }
    // (Leave rest of the blocks alone)

    final int numThreads = 10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);
    final Future<?>[] futures = new Future<?>[numThreads];
    final FileMeta[] fileMetas = new FileMeta[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      futures[index] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            final FileMeta fileMeta = metaManager.get(new Path(path), new User());
            fileMetas[index] = metaManager.loadData(fileMeta);
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

      final List<BlockMeta> updatedBlocks = updatedFileMeta.getBlocks();
      assertEquals(locatedBlocks.getLocatedBlocks().size(), updatedBlocks.size());
      final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
              ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
      assertEquals(numBlocksComputed, updatedBlocks.size());
      for (int i = 0; i < updatedBlocks.size(); i++) {
        assertEquals(locatedBlocks.get(i).getStartOffset(), updatedBlocks.get(i).getOffSet());
        assertEquals(locatedBlocks.get(i).getBlockSize(), updatedBlocks.get(i).getLength());
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
    final String path = TESTDIR + "/largeFile";
    final Path largeFile = ITUtils.writeFile(fs, path, chunkLength, numChunks);

    final LocatedBlocks locatedBlocks = ((DistributedFileSystem) fs)
            .getClient().getLocatedBlocks(largeFile.toString(), 0, chunkLength * numChunks);

    final FileMeta fm = metaManager.get(new Path(path), new User());
    final FileMeta fileMeta = metaManager.loadData(fm);

    final int numThreads = 20;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);
    final Future<?>[] futures = new Future<?>[numThreads];
    final FileMeta[] fileMetas = new FileMeta[numThreads];

    final List<BlockMeta> blocks = fileMeta.getBlocks();
    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      futures[index] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            if (index % 2 == 0) {
              final FileMeta fileMeta = metaManager.get(new Path(path), new User());
              fileMetas[index] = metaManager.loadData(fileMeta);
            } else {
              final BlockMeta block = blocks.get(index);
              final Iterator<NodeInfo> it = block.getLocationsIterator();
              while (it.hasNext()) {
                final NodeInfo nodeInfo = it.next();
                cacheLocationRemover.remove(fileMeta.getFullPath(), new BlockId(block), nodeInfo.getAddress());
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

      final List<BlockMeta> updatedBlocks = updatedFileMeta.getBlocks();
      assertEquals(locatedBlocks.getLocatedBlocks().size(), updatedBlocks.size());
      final int numBlocksComputed = (chunkLength * numChunks) / blockSize +
              ((chunkLength * numChunks) % blockSize == 0 ? 0 : 1); // 1, if there is a remainder
      assertEquals(numBlocksComputed, updatedBlocks.size());
      for (int i = 0; i < updatedBlocks.size(); i++) {
        assertEquals(locatedBlocks.get(i).getStartOffset(), updatedBlocks.get(i).getOffSet());
        assertEquals(locatedBlocks.get(i).getBlockSize(), updatedBlocks.get(i).getLength());
        assertTrue(updatedBlocks.get(i).getLocationsSize() > 0);
        assertTrue(3 >= updatedBlocks.get(i).getLocationsSize());
      }
    }
  }

  /**
   * Verify concurrent creation of files under the same directory
   */
  @Test
  public void testConcurrentCreateFile() throws Throwable {
    final int numFiles = 100;
    final short replication = 1;
    final ExecutorService executorService = Executors.newCachedThreadPool();
    final Future<?>[] futures = new Future<?>[numFiles];
    final Path directoryPath = new Path(TESTDIR, "concurrentFile");

    baseFsClient.mkdirs(directoryPath.toUri().getPath());

    // Concurrently create files under the same directory
    for (int i = 0; i < numFiles; i++) {
      final int index = i;
      futures[index] = executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            final Path filePath = new Path(directoryPath, String.valueOf(index));
            metaManager.createFile(filePath.toUri().getPath(), replication, blockSize);
          } catch (final Throwable t) {
            throw new RuntimeException(t);
          }
        }
      });
    }
    executorService.shutdown();

    // Verify that all operations succeeded
    int successCount = 0;
    for (final Future future : futures) {
      future.get();
      successCount += future.isCancelled() ? 0 : 1;
    }
    assertEquals(numFiles, successCount);

    final FileMeta parentMeta = metaManager.get(directoryPath, new User());

    for (int index = 0; index < numFiles; index++) {
      final Path filePath = new Path(directoryPath, String.valueOf(index));

      // Should not load meta as the fileMeta is already created(cached)
      final FileMeta result = metaManager.get(filePath, new User());

      // Check validity of fileMeta
      assertNotNull(result);
      assertFalse("Should not be a directory", result.isDirectory());
      assertEquals(filePath.toUri().toString(), result.getFullPath());
      assertEquals(replication, result.getReplication());
      assertEquals(blockSize, result.getBlockSize());
      // children-to-parent mapping
      assertEquals(parentMeta, metaManager.getParent(result));
      // parent-to-children mapping
      assertNotNull(metaManager.getChildren(parentMeta));
      assertTrue("Should be a child of directoryPath, index: " + String.valueOf(index),
              metaManager.getChildren(parentMeta).contains(result));
    }
  }

  @Test
  public void testConcurrentCreateDirectory() throws Throwable {
    final int numDirs = 100;
    final ExecutorService executorService = Executors.newCachedThreadPool();
    final Future<Boolean>[] futures = new Future[numDirs];
    final Path rootPath = new Path(TESTDIR, "concurrentDir");

    baseFsClient.mkdirs(rootPath.toUri().getPath());

    // Concurrently create directories under the root directory
    for (int i = 0; i < numDirs; i++) {
      final int index = i;
      futures[index] = executorService.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() {
          try {
            final Path dirPath = new Path(rootPath, String.valueOf(index));
            return metaManager.createDirectory(dirPath.toUri().getPath());
          } catch (final Throwable t) {
            throw new RuntimeException(t);
          }
        }
      });
    }
    executorService.shutdown();

    // Verify that all operations succeeded
    int successCount = 0;
    for (final Future<Boolean> future : futures) {
      assertTrue("CreateDirectory should succeed", future.get());
      successCount += future.isCancelled() ? 0 : 1;
    }
    assertEquals(numDirs, successCount);

    final FileMeta parentMeta = metaManager.get(rootPath, new User());

    // Check the parent has all the children directories.
    final List<FileMeta> children = metaManager.getChildren(metaManager.get(rootPath, new User()));
    assertEquals(numDirs, children.size());
    for (FileMeta childMeta : children) {
      assertTrue("Should be a directory", childMeta.isDirectory());
      assertEquals(parentMeta, metaManager.getParent(childMeta));
    }

    // Check the created directories to have the parent.
    for (int index = 0; index < numDirs; index++) {
      final Path filePath = new Path(rootPath, String.valueOf(index));

      final FileMeta childMeta = metaManager.get(filePath, new User());
      assertEquals(parentMeta, metaManager.getParent(childMeta));
      assertTrue("Should be a child of rootPath, index: " + String.valueOf(index),
              children.contains(childMeta));
      assertTrue("Should be a directory", childMeta.isDirectory());
      assertEquals(filePath.toUri().getPath(), childMeta.getFullPath());
    }
  }
}
