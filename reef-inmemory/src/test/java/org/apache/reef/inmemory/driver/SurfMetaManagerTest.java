package org.apache.reef.inmemory.driver;

import org.apache.reef.inmemory.common.*;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.driver.metatree.MetaTree;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test class for SurfMetaManager
 */
public final class SurfMetaManagerTest {
  private static final long blockSize = 128L * 1024 * 1024;
  private static final long fileSize = blockSize * 2;
  private static final long blockLength = blockSize / 2;
  private CacheNodeMessenger cacheNodeMessenger;
  private CacheLocationRemover cacheLocationRemover;
  private FileMetaUpdater fileMetaUpdater;
  private BlockMetaFactory blockMetaFactory;

  /**
   * Setup the Meta Manager with a mock CacheLoader that returns
   * blank metadata for each path.
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    cacheNodeMessenger = mockCacheMessenger();
    cacheLocationRemover = new CacheLocationRemover();
    fileMetaUpdater = mock(FileMetaUpdater.class);
    blockMetaFactory = mock(BlockMetaFactory.class);
  }

  private static CacheNodeMessenger mockCacheMessenger() {
    final CacheNodeMessenger cacheNodeMessenger = mock(CacheNodeMessenger.class);
    doNothing().when(cacheNodeMessenger).clearAll();
    return cacheNodeMessenger;
  }

  /**
   * Test that removal and failure updates are reflected in the cacheLocationRemover.
   */
  @Test
  public void testApplyUpdates() throws Throwable {
    final MetaTree metaTree = mock(MetaTree.class);
    final SurfMetaManager metaManager =
            new SurfMetaManager(cacheNodeMessenger, cacheLocationRemover, fileMetaUpdater, blockMetaFactory, metaTree);
    final String[] addresses = new String[]{ "localhost:17001", "localhost:17002", "localhost:17003" };
    final String pathA = "/path/fileA";
    final long pathAFileId = 1;

    {
      final long[] blockOffsets = new long[]{0, 1, 2, 3};
      final FileMeta fileMeta = fileMeta(pathAFileId, blockOffsets, addresses);

      when(metaTree.getOrLoadFileMeta(pathA)).thenReturn(fileMeta);
      when(fileMetaUpdater.update(eq(pathA), eq(fileMeta))).thenReturn(fileMeta.deepCopy());
      assertEquals(fileMeta, metaManager.load(pathA));
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addRemoval(updates, pathAFileId, 0);
      addRemoval(updates, pathAFileId, 1);
      addRemoval(updates, pathAFileId, 2);
      addRemoval(updates, pathAFileId, 3);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[0]);
      metaManager.applyCacheNodeUpdates(cacheNode, updates);

      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathAFileId);
      assertEquals(4, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(1, pendingRemoves.get(blockId).size());
      }
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addFailure(updates, pathAFileId, 0);
      addFailure(updates, pathAFileId, 1);
      addRemoval(updates, pathAFileId, 2);
      addRemoval(updates, pathAFileId, 3);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[1]);
      metaManager.applyCacheNodeUpdates(cacheNode, updates);

      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathAFileId);
      assertEquals(4, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(1, pendingRemoves.get(blockId).size());
      }
    }
  }

  /**
   * Test that removal and failure updates are reflected in the cacheLocationRemover,
   * under concurrent operations.
   */
  @Test
  public void testConcurrentUpdates() throws Throwable {
    final MetaTree metaTree = mock(MetaTree.class);
    final SurfMetaManager metaManager =
            new SurfMetaManager(cacheNodeMessenger, cacheLocationRemover, fileMetaUpdater, blockMetaFactory, metaTree);

    final int numNodes = 10;
    int port = 17000;
    final String[] addresses = new String[numNodes];
    for (int i = 0; i < numNodes; i++) {
      addresses[i] = "localhost:" + (port++);
    }

    final int numBlocks = 200;
    final String pathA = "/path/fileA";
    final long pathAFileId = 1;
    {
      final long[] blockOffsets = new long[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        blockOffsets[i] = i;
      }
      final FileMeta fileMeta = fileMeta(pathAFileId, blockOffsets, addresses);
      when(metaTree.getOrLoadFileMeta(pathA)).thenReturn(fileMeta);
      when(fileMetaUpdater.update(eq(pathA), eq(fileMeta))).thenReturn(fileMeta.deepCopy());

      assertEquals(fileMeta, metaManager.load(pathA));
    }

    final String pathB = "/path/fileB";
    final long pathBFileId = 2;
    {
      final long[] blockOffsets = new long[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        blockOffsets[i] = numBlocks + i;
      }
      final FileMeta fileMeta = fileMeta(pathBFileId, blockOffsets, addresses);
      when(metaTree.getOrLoadFileMeta(pathB)).thenReturn(fileMeta);
      when(fileMetaUpdater.update(eq(pathB), eq(fileMeta))).thenReturn(fileMeta.deepCopy());

      assertEquals(fileMeta, metaManager.load(pathB));
    }

    final ExecutorService es = Executors.newFixedThreadPool(numNodes * 2);
    for (int i = 0; i < numNodes; i++) {
      final String address = addresses[i];
      es.submit(new Runnable() {
        @Override
        public void run() {
          final CacheUpdates updates = new CacheUpdates();
          for (int j = 0; j < numBlocks; j++) {
            addRemoval(updates, pathAFileId, j);
          }
          final CacheNode cacheNode = mock(CacheNode.class);
          when(cacheNode.getAddress()).thenReturn(address);
          metaManager.applyCacheNodeUpdates(cacheNode, updates);
        }
      });
      es.submit(new Runnable() {
        @Override
        public void run() {
          final CacheUpdates updates = new CacheUpdates();
          for (int j = 0; j < numBlocks; j++) {
            addRemoval(updates, pathBFileId, numBlocks + j);
          }
          final CacheNode cacheNode = mock(CacheNode.class);
          when(cacheNode.getAddress()).thenReturn(address);
          metaManager.applyCacheNodeUpdates(cacheNode, updates);
        }
      });
    }
    es.shutdown();
    final boolean terminated = es.awaitTermination(60, TimeUnit.SECONDS);
    assertTrue(terminated);

    {
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathAFileId);
      assertEquals(numBlocks, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(10, pendingRemoves.get(blockId).size());
      }
    }

    {
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathBFileId);
      assertEquals(numBlocks, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(10, pendingRemoves.get(blockId).size());
      }
    }
  }

  private FileMeta fileMeta(final long fileId, final long[] offsets, final String[] locations) {
    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFileId(fileId);
    fileMeta.setFileSize(fileSize);
    fileMeta.setBlockSize(blockSize);
    for (final long offset : offsets) {
      final BlockMeta blockMeta = new BlockMeta();
      fileMeta.addToBlocks(blockMeta);
      blockMeta.setFileId(fileId);
      blockMeta.setLength(blockLength);
      blockMeta.setOffSet(offset);
      for (final String location : locations) {
        final NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.setAddress(location);
        nodeInfo.setRack("/default");
        blockMeta.addToLocations(nodeInfo);
      }
    }
    return fileMeta;
  }

  private static void addRemoval(final CacheUpdates updates, final long fileId, final long offset) {
    final BlockId blockId = new BlockId(fileId, offset);
    updates.addRemoval(blockId);
  }

  private static void addFailure(final CacheUpdates updates, final long fileId, final long offset) {
    final BlockId blockId = new BlockId(fileId, offset);
    updates.addFailure(blockId, new IOException("Test"));
  }
}
