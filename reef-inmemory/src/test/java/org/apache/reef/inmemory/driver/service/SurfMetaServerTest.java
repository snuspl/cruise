package org.apache.reef.inmemory.driver.service;

import com.google.common.cache.LoadingCache;
import org.apache.reef.inmemory.driver.CacheManager;
import org.apache.reef.inmemory.driver.CacheMessenger;
import org.apache.reef.inmemory.driver.SurfMetaManager;
import org.junit.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for SurfMetaServer
 */
public final class SurfMetaServerTest {

  /**
   * Test that java.io.FileNotFoundException is properly thrown as a thrift IDL defined
   * exception that can be returned to the client.
   * @throws Throwable
   */
  @Test
  public void testExceptionTranslation() throws Throwable {

    final LoadingCache loadingCache = mock(LoadingCache.class);
    when(loadingCache.get(anyObject())).thenThrow(java.io.FileNotFoundException.class);
    final CacheMessenger cacheMessenger = mock(CacheMessenger.class);
    final CacheManager cacheManager = mock(CacheManager.class);

    final SurfMetaManager metaManager = new SurfMetaManager(loadingCache, cacheMessenger);

    try {
      final SurfMetaServer metaService = new SurfMetaServer(metaManager, cacheManager, 18000, 10, 1);
      metaService.load("/nonexistent/path");
    } catch (Exception e) {
      assertTrue("Unexpected exception "+e, e instanceof org.apache.reef.inmemory.driver.exceptions.FileNotFoundException);
    }
    assertEquals(0, metaManager.clear());
  }
}
