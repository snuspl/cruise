package org.apache.reef.inmemory.fs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * Test class for SurfMetaManager
 */
public final class SurfMetaManagerTest extends TestCase {
  SurfMetaManager metaManager;
  CacheLoader<Path, FileMeta> cacheLoader;
  CacheMessenger cacheMessenger;
  Path path;
  User user;

  /**
   * Setup the Meta Manager with a mock CacheLoader that returns
   * blank metadata for each path.
   * @throws Exception
   */
  @Override
  public void setUp() throws Exception {
    path = new Path("/path");

    cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(path)).thenReturn(new FileMeta());

    cacheMessenger = mock(CacheMessenger.class);
    doNothing().when(cacheMessenger).clearAll();

    LoadingCache<Path, FileMeta> cache = CacheBuilder.newBuilder()
            .concurrencyLevel(4)
            .build(cacheLoader);

    metaManager = new SurfMetaManager(cache, cacheMessenger);
    user = new User();
    user.setId("surf");
    user.setGroup("surf");
  }

  /**
   * Verify that load is called only when the path given does not exist.
   * @throws Throwable
   */
  @Test
  public void testGet() throws Throwable {
    metaManager.getBlocks(path, user);
    verify(cacheLoader, times(1)).load(path);
    metaManager.getBlocks(path, user);
    verify(cacheLoader, times(1)).load(path);
  }

  /**
   * Verify that clear properly clears the cache, and returns the number of
   * previously loaded paths.
   * @throws Throwable
   */
  @Test
  public void testClear() throws Throwable {
    assertEquals(0, metaManager.clear());
    metaManager.getBlocks(path, user);
    assertEquals(1, metaManager.clear());
    assertEquals(0, metaManager.clear());
  }
}
