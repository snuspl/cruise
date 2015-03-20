package org.apache.reef.inmemory.driver.service;

import com.google.common.cache.LoadingCache;
import org.apache.reef.inmemory.common.BlockMetaFactory;
import org.apache.reef.inmemory.common.FileMetaFactory;
import org.apache.reef.inmemory.driver.*;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.apache.reef.inmemory.driver.write.WritingCacheSelectionPolicy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
    final CacheNodeMessenger cacheNodeMessenger = mock(CacheNodeMessenger.class);
    final CacheNodeManager cacheNodeManager = mock(CacheNodeManager.class);
    final ServiceRegistry serviceRegistry = mock(ServiceRegistry.class);
    final ReplicationPolicy replicationPolicy = mock(ReplicationPolicy.class);
    final WritingCacheSelectionPolicy writingCacheSelectionPolicy = mock(WritingCacheSelectionPolicy.class);
    final CacheLocationRemover cacheLocationRemover = new CacheLocationRemover();
    final FileMetaUpdater fileMetaUpdater = mock(FileMetaUpdater.class);
    final BlockMetaFactory blockMetaFactory = mock(BlockMetaFactory.class);
    final FileMetaFactory metaFactory = mock(FileMetaFactory.class);
    final LocationSorter locationSorter = mock(LocationSorter.class);
    final BaseFsClient baseFsClient = mock(BaseFsClient.class);

    final SurfMetaManager metaManager = new SurfMetaManager(loadingCache, cacheNodeMessenger, cacheLocationRemover,
            fileMetaUpdater, blockMetaFactory, metaFactory, locationSorter, baseFsClient);

    try {
      final SurfMetaServer metaService = new SurfMetaServer(
              metaManager, cacheNodeManager, serviceRegistry, writingCacheSelectionPolicy, replicationPolicy, 18000, 10, 1);
      metaService.load("/nonexistent/path");
    } catch (Exception e) {
      assertTrue("Unexpected exception "+e, e instanceof org.apache.reef.inmemory.common.exceptions.FileNotFoundException);
    }
    assertEquals(0, metaManager.clear());
  }
}
