package org.apache.reef.inmemory.driver.hdfs;

import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.reef.inmemory.driver.CacheManager;
import org.apache.reef.inmemory.driver.CacheManagerImpl;
import org.apache.reef.inmemory.driver.TestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class HdfsCacheManagerTest {

  @Test
  public void testBindPortUpdate() throws IOException {
    final HdfsCacheSelectionPolicy selector = mock(HdfsCacheSelectionPolicy.class);
    final CacheManager manager = new CacheManagerImpl(mock(EvaluatorRequestor.class), "test", 0, 0, 0, 0);

    final RunningTask task = TestUtils.mockRunningTask("a", "hosta");

    manager.addRunningTask(task);
    assertEquals("Expected task not added when port unassigned", 0, manager.getCaches().size());

    manager.handleHeartbeat(task.getId(), TestUtils.cacheStatusMessage(18001));
    assertEquals(1, manager.getCaches().size());
    assertEquals("hosta:18001", manager.getCaches().get(0).getAddress());

    manager.handleHeartbeat(task.getId(), TestUtils.cacheStatusMessage(18001));
    assertEquals(1, manager.getCaches().size());
    assertEquals("hosta:18001", manager.getCaches().get(0).getAddress());
  }
}
