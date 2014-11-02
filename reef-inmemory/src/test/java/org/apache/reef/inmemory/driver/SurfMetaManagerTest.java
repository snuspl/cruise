package org.apache.reef.inmemory.driver;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.common.MockBlockId;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test class for SurfMetaManager
 */
public final class SurfMetaManagerTest {
  private static final long blockSize = 128L * 1024 * 1024;
  private CacheMessenger cacheMessenger;
  private CacheLocationRemover cacheLocationRemover;
  private CacheUpdater cacheUpdater;
  private LocationSorter locationSorter;

  /**
   * Setup the Meta Manager with a mock CacheLoader that returns
   * blank metadata for each path.
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    cacheMessenger = mockCacheMessenger();
    cacheLocationRemover = new CacheLocationRemover();
    cacheUpdater = mock(CacheUpdater.class);
    locationSorter = mock(LocationSorter.class);
  }

  private static User defaultUser() {
    final User user = new User();
    user.setId("surf");
    user.setGroup("surf");
    return user;
  }

  private static CacheMessenger mockCacheMessenger() {
    final CacheMessenger cacheMessenger = mock(CacheMessenger.class);
    doNothing().when(cacheMessenger).clearAll();
    return cacheMessenger;
  }

  /**
   * Verify that load is called only when the path given does not exist.
   * @throws Throwable
   */
  @Test
  public void testGet() throws Throwable {
    final Path path = new Path("/path");
    final User user = defaultUser();

    final CacheLoader<Path, FileMeta> cacheLoader = mock(CacheLoader.class);
    final FileMeta fileMeta = new FileMeta();
    when(cacheLoader.load(path)).thenReturn(fileMeta);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater, locationSorter);
    when(cacheUpdater.updateMeta(any(Path.class), eq(fileMeta))).thenReturn(fileMeta.deepCopy());

    metaManager.getFile(path, user);
    verify(cacheLoader, times(1)).load(path);
    metaManager.getFile(path, user);
    verify(cacheLoader, times(1)).load(path);
  }

  /**
   * Verify that clear properly clears the cache, and returns the number of
   * previously loaded paths.
   * @throws Throwable
   */
  @Test
  public void testClear() throws Throwable {
    final Path path = new Path("/path");
    final User user = defaultUser();

    final CacheLoader<Path, FileMeta> cacheLoader = mock(CacheLoader.class);
    final FileMeta fileMeta = new FileMeta();
    when(cacheLoader.load(path)).thenReturn(fileMeta);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater, locationSorter);
    when(cacheUpdater.updateMeta(any(Path.class), eq(fileMeta))).thenReturn(fileMeta.deepCopy());

    assertEquals(0, metaManager.clear());
    metaManager.getFile(path, user);
    assertEquals(1, metaManager.clear());
    assertEquals(0, metaManager.clear());
  }

  private FileMeta fileMeta(final Path path, final long[] blockIds, final String[] locations) {
    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(path.toString());
    fileMeta.setBlockSize(blockSize);
    for (final long blockId : blockIds) {
      final BlockInfo blockInfo = new BlockInfo();
      fileMeta.addToBlocks(blockInfo);
      blockInfo.setFilePath(path.toString());
      blockInfo.setBlockId(blockId);
      for (final String location : locations) {
        final NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.setAddress(location);
        nodeInfo.setRack("/default");
        blockInfo.addToLocations(nodeInfo);
      }
    }
    return fileMeta;
  }

  private static void addRemoval(final CacheUpdates updates, final Path path, final long blockSize, final long uniqueId) {
    final BlockId blockId = new MockBlockId(uniqueId, blockSize, path.toString());
    updates.addRemoval(blockId);
  }

  private static void addFailure(final CacheUpdates updates, final Path path, final long blockSize, final long uniqueId) {
    final BlockId blockId = new MockBlockId(uniqueId, blockSize, path.toString());
    updates.addFailure(blockId, new IOException("Test"));
  }

  /**
   * Test that removal and failure updates are reflected in the cacheLocationRemover.
   */
  @Test
  public void testApplyUpdates() throws Throwable {
    final CacheLoader<Path, FileMeta> cacheLoader = mock(CacheLoader.class);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater, locationSorter);

    final String[] addresses = new String[]{ "localhost:17001", "localhost:17002", "localhost:17003" };
    final User user = defaultUser();

    final Path pathA = new Path("/path/fileA");
    {
      final long[] blockIds = new long[]{0, 1, 2, 3};
      final FileMeta fileMeta = fileMeta(pathA, blockIds, addresses);
      when(cacheLoader.load(pathA)).thenReturn(fileMeta);
      when(cacheUpdater.updateMeta(any(Path.class), eq(fileMeta))).thenReturn(fileMeta.deepCopy());

      assertEquals(fileMeta, metaManager.getFile(pathA, user));
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addRemoval(updates, pathA, blockSize, 0);
      addRemoval(updates, pathA, blockSize, 1);
      addRemoval(updates, pathA, blockSize, 2);
      addRemoval(updates, pathA, blockSize, 3);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[0]);
      metaManager.applyUpdates(cacheNode, updates);

      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathA.toString());
      assertEquals(4, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(1, pendingRemoves.get(blockId).size());
      }
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addFailure(updates, pathA, blockSize, 0);
      addFailure(updates, pathA, blockSize, 1);
      addRemoval(updates, pathA, blockSize, 2);
      addRemoval(updates, pathA, blockSize, 3);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[1]);
      metaManager.applyUpdates(cacheNode, updates);

      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathA.toString());
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
    final CacheLoader<Path, FileMeta> cacheLoader = mock(CacheLoader.class);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater, locationSorter);

    final int numNodes = 10;
    int port = 17000;
    final String[] addresses = new String[numNodes];
    for (int i = 0; i < numNodes; i++) {
      addresses[i] = "localhost:"+(port++);
    }
    final User user = defaultUser();

    final int numBlocks = 200;
    final Path pathA = new Path("/path/fileA");
    {
      final long[] blockIds = new long[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        blockIds[i] = i;
      }
      final FileMeta fileMeta = fileMeta(pathA, blockIds, addresses);
      when(cacheLoader.load(pathA)).thenReturn(fileMeta);
      when(cacheUpdater.updateMeta(any(Path.class), eq(fileMeta))).thenReturn(fileMeta.deepCopy());

      assertEquals(fileMeta, metaManager.getFile(pathA, user));
    }

    final Path pathB = new Path("/path/fileB");
    {
      final long[] blockIds = new long[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        blockIds[i] = numBlocks + i;
      }
      final FileMeta fileMeta = fileMeta(pathB, blockIds, addresses);
      when(cacheLoader.load(pathB)).thenReturn(fileMeta);
      when(cacheUpdater.updateMeta(any(Path.class), eq(fileMeta))).thenReturn(fileMeta.deepCopy());
      assertEquals(fileMeta, metaManager.getFile(pathB, user));
    }

    final ExecutorService es = Executors.newFixedThreadPool(numNodes * 2);
    for (int i = 0; i < numNodes; i++) {
      final String address = addresses[i];
      es.submit(new Runnable() {
        @Override
        public void run() {
          final CacheUpdates updates = new CacheUpdates();
          for (int j = 0; j < numBlocks; j++) {
            addRemoval(updates, pathA, blockSize, j);
          }
          final CacheNode cacheNode = mock(CacheNode.class);
          when(cacheNode.getAddress()).thenReturn(address);
          metaManager.applyUpdates(cacheNode, updates);
        }
      });
      es.submit(new Runnable() {
        @Override
        public void run() {
          final CacheUpdates updates = new CacheUpdates();
          for (int j = 0; j < numBlocks; j++) {
            addRemoval(updates, pathB, blockSize, numBlocks + j);
          }
          final CacheNode cacheNode = mock(CacheNode.class);
          when(cacheNode.getAddress()).thenReturn(address);
          metaManager.applyUpdates(cacheNode, updates);
        }
      });
    }
    es.shutdown();
    final boolean terminated = es.awaitTermination(60, TimeUnit.SECONDS);
    assertTrue(terminated);

    {
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathA.toString());
      assertEquals(numBlocks, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(10, pendingRemoves.get(blockId).size());
      }
    }

    {
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathB.toString());
      assertEquals(numBlocks, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(10, pendingRemoves.get(blockId).size());
      }
    }
  }
}
