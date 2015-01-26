package org.apache.reef.inmemory.driver.service;

import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.reef.inmemory.common.BlockIdFactory;
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
    final CacheMessenger cacheMessenger = mock(CacheMessenger.class);
    final CacheManager cacheManager = mock(CacheManager.class);
    final ServiceRegistry serviceRegistry = mock(ServiceRegistry.class);
    final ReplicationPolicy replicationPolicy = mock(ReplicationPolicy.class);
    final WritingCacheSelectionPolicy writingCacheSelectionPolicy = mock(WritingCacheSelectionPolicy.class);
    final CacheLocationRemover cacheLocationRemover = new CacheLocationRemover();
    final CacheUpdater cacheUpdater = mock(CacheUpdater.class);
    final BlockIdFactory blockIdFactory = mock(BlockIdFactory.class);
    final LocationSorter locationSorter = mock(LocationSorter.class);
    final DFSClient dfsClient = mock(DFSClient.class);

    final SurfMetaManager metaManager = new SurfMetaManager(loadingCache, cacheMessenger, cacheLocationRemover, cacheUpdater, blockIdFactory, locationSorter, dfsClient);

    try {
      final SurfMetaServer metaService = new SurfMetaServer(
              metaManager, cacheManager, serviceRegistry, writingCacheSelectionPolicy, replicationPolicy, 18000, 10, 1);
      metaService.load("/nonexistent/path");
    } catch (Exception e) {
      assertTrue("Unexpected exception "+e, e instanceof org.apache.reef.inmemory.common.exceptions.FileNotFoundException);
    }
    assertEquals(0, metaManager.clear());
  }
}
