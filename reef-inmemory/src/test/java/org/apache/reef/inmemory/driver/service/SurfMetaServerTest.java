package org.apache.reef.inmemory.driver.service;

import org.apache.reef.inmemory.common.BlockMetaFactory;
import org.apache.reef.inmemory.driver.*;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.driver.metatree.MetaTree;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.apache.reef.inmemory.driver.write.WritingCacheSelectionPolicy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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

    final CacheNodeMessenger cacheNodeMessenger = mock(CacheNodeMessenger.class);
    final CacheNodeManager cacheNodeManager = mock(CacheNodeManager.class);
    final ServiceRegistry serviceRegistry = mock(ServiceRegistry.class);
    final ReplicationPolicy replicationPolicy = mock(ReplicationPolicy.class);
    final WritingCacheSelectionPolicy writingCacheSelectionPolicy = mock(WritingCacheSelectionPolicy.class);
    final CacheLocationRemover cacheLocationRemover = new CacheLocationRemover();
    final FileMetaUpdater fileMetaUpdater = mock(FileMetaUpdater.class);
    final BlockMetaFactory blockMetaFactory = mock(BlockMetaFactory.class);
    final LocationSorter locationSorter = mock(LocationSorter.class);
    final MetaTree metaTree = mock(MetaTree.class);

    final SurfMetaManager metaManager = new SurfMetaManager(cacheNodeMessenger, cacheLocationRemover,
            fileMetaUpdater, blockMetaFactory, metaTree);

    try {
      final SurfMetaServer metaService = new SurfMetaServer(
              metaManager, cacheNodeManager, serviceRegistry, writingCacheSelectionPolicy, replicationPolicy, locationSorter, 18000, 10, 1);
      metaService.load("/nonexistent/path");
    } catch (Exception e) {
      assertTrue("Unexpected exception "+e, e instanceof org.apache.reef.inmemory.common.exceptions.FileNotFoundException);
    }
    assertEquals(0, metaManager.clear());
  }
}
