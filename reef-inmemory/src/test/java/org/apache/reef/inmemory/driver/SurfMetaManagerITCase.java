package org.apache.reef.inmemory.driver;

import org.apache.reef.driver.task.RunningTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockInfoFactory;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMetaFactory;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.common.instrumentation.NullEventRecorder;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.driver.hdfs.*;
import org.apache.reef.inmemory.driver.metatree.MetaTree;
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

  private FileSystem fs;
  private SurfMetaManager metaManager;
  private FileSystem baseFs;
  private CacheLocationRemover cacheLocationRemover;

  @Before
  public void setUp() throws IOException {
    final EventRecorder RECORD = new NullEventRecorder();
    final CacheNodeManager manager = TestUtils.cacheManager();
    final HdfsCacheNodeMessenger messenger = new HdfsCacheNodeMessenger(manager);
    final HdfsCacheSelectionPolicy selector = new HdfsRandomCacheSelectionPolicy();
    cacheLocationRemover = new CacheLocationRemover();
    final HdfsBlockMetaFactory blockMetaFactory = new HdfsBlockMetaFactory();
    final HdfsBlockInfoFactory blockInfoFactory = new HdfsBlockInfoFactory();
    final ReplicationPolicy replicationPolicy = mock(ReplicationPolicy.class);

    for (int i = 0; i < 3; i++) {
      final RunningTask task = TestUtils.mockRunningTask("" + i, "host" + i);

      manager.addRunningTask(task);
      manager.handleHeartbeat(task.getId(), TestUtils.cacheStatusMessage(18001 + i));
    }
    List<CacheNode> selectedNodes = manager.getCaches();
    assertEquals(3, selectedNodes.size());
    when(replicationPolicy.getReplicationAction(anyString(), any(FileMeta.class))).thenReturn(new Action(3, false));

    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);

    fs = ITUtils.getHdfs(hdfsConfig);
    fs.mkdirs(new Path(TESTDIR));

    baseFs = new BaseFsConstructor(ITUtils.getBaseFsAddress()).newInstance();
    final HdfsBlockLocationGetter blockLocationGetter = new HdfsBlockLocationGetter(baseFs);
    final HDFSClient baseFsClient = new HDFSClient(baseFs);
    final MetaTree metaTree = new MetaTree(baseFsClient, RECORD);

    final FileMetaUpdater fileMetaUpdater = new HdfsFileMetaUpdater(manager, messenger, selector, cacheLocationRemover, blockMetaFactory, blockInfoFactory, replicationPolicy, baseFs, blockLocationGetter);

    metaManager = new SurfMetaManager(messenger, cacheLocationRemover, fileMetaUpdater, blockMetaFactory, metaTree);
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
            fileMetas[index] = metaManager.load(path);
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

    // They should all return different fileMetas (thanks to deepCopy())
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
    assertEquals(chunkLength * numChunks, fileMeta.getFileSize());

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

    final FileMeta fileMeta = metaManager.load(path);

    final List<BlockMeta> blocks = fileMeta.getBlocks();
    // Remove first location from first block
    {
      final BlockMeta block = blocks.get(0);
      final Iterator<NodeInfo> it = block.getLocationsIterator();
      final NodeInfo nodeInfo = it.next();
      cacheLocationRemover.remove(fileMeta.getFileId(), new BlockId(block), nodeInfo.getAddress());
    }
    // Remove all locations from second block
    {
      final BlockMeta block = blocks.get(1);
      final Iterator<NodeInfo> it = block.getLocationsIterator();
      while (it.hasNext()) {
        final NodeInfo nodeInfo = it.next();
        cacheLocationRemover.remove(fileMeta.getFileId(), new BlockId(block), nodeInfo.getAddress());
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
            fileMetas[index] = metaManager.load(path);
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

    final FileMeta fileMeta = metaManager.load(path);
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
              fileMetas[index] = metaManager.load(path);
            } else {
              final BlockMeta block = blocks.get(index);
              final Iterator<NodeInfo> it = block.getLocationsIterator();
              while (it.hasNext()) {
                final NodeInfo nodeInfo = it.next();
                cacheLocationRemover.remove(fileMeta.getFileId(), new BlockId(block), nodeInfo.getAddress());
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
}
