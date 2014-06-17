package org.apache.reef.inmemory.fs.service;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.SurfMetaManager;
import org.apache.reef.inmemory.fs.entity.User;
import org.junit.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
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
    SurfMetaManager metaManager = mock(SurfMetaManager.class);
    when(metaManager.getBlocks(any(Path.class), any(User.class))).thenThrow(java.io.FileNotFoundException.class);

    try {
      SurfMetaServer metaService = new SurfMetaServer(metaManager, 18000, 10, 1);
      metaService.load("/nonexistent/path");
    } catch (Exception e) {
      assertTrue("Unexpected exception "+e, e instanceof org.apache.reef.inmemory.fs.exceptions.FileNotFoundException);
    }
    assertEquals(0, metaManager.clear());
  }
}
