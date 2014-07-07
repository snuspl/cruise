package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.RunningTask;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class HdfsCacheManagerTest {

  @Test
  public void testBindPortUpdate() throws IOException {
    final HdfsCacheSelectionPolicy selector = mock(HdfsCacheSelectionPolicy.class);
    final CacheManager manager = new CacheManagerImpl(mock(EvaluatorRequestor.class), "test", 0, 0, 0, 0);

    final RunningTask task = TestUtils.mockRunningTask("a", "hosta");

    manager.addRunningTask(task);
    assertEquals("Expected cache not added when port unassigned", 0, manager.getCaches().size());

    manager.handleUpdate(task.getId(), TestUtils.cacheStatusMessage(18001));
    assertEquals(1, manager.getCaches().size());
    assertEquals("hosta:18001", manager.getCaches().get(0).getAddress());

    manager.handleUpdate(task.getId(), TestUtils.cacheStatusMessage(18001));
    assertEquals(1, manager.getCaches().size());
    assertEquals("hosta:18001", manager.getCaches().get(0).getAddress());
  }
}
