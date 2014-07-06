package org.apache.reef.inmemory.fs.service;

import com.google.common.cache.LoadingCache;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.SurfMetaManager;
import org.apache.reef.inmemory.fs.TaskManager;
import org.apache.reef.inmemory.fs.entity.User;
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

    LoadingCache loadingCache = mock(LoadingCache.class);
    when(loadingCache.get(anyObject())).thenThrow(java.io.FileNotFoundException.class);
    TaskManager taskManager = mock(TaskManager.class);
    EvaluatorRequestor evaluatorRequestor = mock(EvaluatorRequestor.class);

    SurfMetaManager metaManager = new SurfMetaManager(loadingCache, taskManager);

    try {
      SurfMetaServer metaService = new SurfMetaServer(metaManager, evaluatorRequestor, 18000, 10, 1);
      metaService.load("/nonexistent/path");
    } catch (Exception e) {
      assertTrue("Unexpected exception "+e, e instanceof org.apache.reef.inmemory.fs.exceptions.FileNotFoundException);
    }
    assertEquals(0, metaManager.clear());
  }
}
