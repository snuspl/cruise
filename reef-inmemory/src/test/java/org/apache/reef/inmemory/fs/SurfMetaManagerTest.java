package org.apache.reef.inmemory.fs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Test class for SurfMeta
 */
public class SurfMetaManagerTest extends TestCase {
  SurfMetaManager metaManager;
  CacheLoader<Path, FileMeta> cacheLoader;
  Path path;
  User user;

  @Override
  public void setUp() throws Exception {
    path = new Path("/path");

    cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(path)).thenReturn(new FileMeta());

    LoadingCache<Path, FileMeta> cache = CacheBuilder.newBuilder()
            .concurrencyLevel(4)
            .build(cacheLoader);

    metaManager = new SurfMetaManager(cache);
    user = new User();
    user.setId("surf");
    user.setGroup("surf");
  }

  @Test
  public void testGet() throws Throwable {
    metaManager.getBlocks(path, user);
    verify(cacheLoader, times(1)).load(path);
    metaManager.getBlocks(path, user);
    verify(cacheLoader, times(1)).load(path);
  }

  @Test
  public void testClear() throws Throwable {
    assertEquals(0, metaManager.clear());
    metaManager.getBlocks(path, user);
    assertEquals(1, metaManager.clear());
  }
}
