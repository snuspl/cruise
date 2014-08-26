package org.apache.reef.inmemory.driver;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.task.BlockId;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test class for SurfMetaManager
 */
public final class SurfMetaManagerTest {
  CacheMessenger cacheMessenger;

  /**
   * Setup the Meta Manager with a mock CacheLoader that returns
   * blank metadata for each path.
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    cacheMessenger = mockCacheMessenger();
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
    when(cacheLoader.load(path)).thenReturn(new FileMeta());
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger);

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
    when(cacheLoader.load(path)).thenReturn(new FileMeta());
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger);

    assertEquals(0, metaManager.clear());
    metaManager.getFile(path, user);
    assertEquals(1, metaManager.clear());
    assertEquals(0, metaManager.clear());
  }

  private FileMeta getFileMeta(final Path path, final long[] blockIds, final String[] locations) {
    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(path.toString());
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

  private static void addRemoval(final CacheUpdates updates, final Path path, final long uniqueId) {
    final BlockId blockId = mock(BlockId.class);
    when(blockId.getFilePath()).thenReturn(path.toString());
    when(blockId.getUniqueId()).thenReturn(uniqueId);
    updates.addRemoval(blockId);
  }

  private static void addFailure(final CacheUpdates updates, final Path path, final long uniqueId) {
    final BlockId blockId = mock(BlockId.class);
    when(blockId.getFilePath()).thenReturn(path.toString());
    when(blockId.getUniqueId()).thenReturn(uniqueId);
    updates.addFailure(blockId, new IOException("Test"));
  }

  @Test
  public void testApplyUpdates() throws Throwable {
    final CacheLoader<Path, FileMeta> cacheLoader = mock(CacheLoader.class);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger);

    final String[] addresses = new String[]{ "localhost:17001", "localhost:17002", "localhost:17003" };
    final User user = defaultUser();

    final Path pathA = new Path("/path/fileA");
    {
      final long[] blockIds = new long[]{1, 2, 3, 4};
      final FileMeta fileMeta = getFileMeta(pathA, blockIds, addresses);
      when(cacheLoader.load(pathA)).thenReturn(fileMeta);
      assertEquals(fileMeta, metaManager.getFile(pathA, user));
    }

    final Path pathB = new Path("/path/fileB");
    {
      final long[] blockIds = new long[]{5, 6};
      final FileMeta fileMeta = getFileMeta(pathB, blockIds, addresses);
      when(cacheLoader.load(pathB)).thenReturn(fileMeta);
      assertEquals(fileMeta, metaManager.getFile(pathB, user));
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addRemoval(updates, pathA, 1);
      addRemoval(updates, pathA, 2);
      addRemoval(updates, pathA, 3);
      addRemoval(updates, pathA, 4);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[0]);
      metaManager.applyUpdates(cacheNode, updates);
      final FileMeta fileMeta = cache.getIfPresent(pathA);
      assertEquals(4, fileMeta.getBlocksSize());
      for (final BlockInfo blockInfo : fileMeta.getBlocks()) {
        assertEquals(2, blockInfo.getLocationsSize());
        for (final NodeInfo nodeInfo : blockInfo.getLocations()) {
          assertNotEquals(addresses[0], nodeInfo.getAddress());
        }
      }
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addFailure(updates, pathA, 1);
      addFailure(updates, pathA, 2);
      addRemoval(updates, pathA, 3);
      addRemoval(updates, pathA, 4);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[1]);
      metaManager.applyUpdates(cacheNode, updates);
      final FileMeta fileMeta = cache.getIfPresent(pathA);
      assertEquals(4, fileMeta.getBlocksSize());
      for (final BlockInfo blockInfo : fileMeta.getBlocks()) {
        assertEquals(1, blockInfo.getLocationsSize());
        for (final NodeInfo nodeInfo : blockInfo.getLocations()) {
          assertEquals(addresses[2], nodeInfo.getAddress());
        }
      }
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addRemoval(updates, pathA, 1);
      addRemoval(updates, pathA, 2);
      addRemoval(updates, pathA, 3);
      addRemoval(updates, pathA, 4);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[2]);
      metaManager.applyUpdates(cacheNode, updates);
      final FileMeta fileMeta = cache.getIfPresent(pathA);
      assertEquals(4, fileMeta.getBlocksSize());
      for (final BlockInfo blockInfo : fileMeta.getBlocks()) {
        assertEquals(0, blockInfo.getLocationsSize());
      }
    }
  }
}
