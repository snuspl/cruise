package org.apache.reef.inmemory.fs;

import com.google.common.cache.CacheStats;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.reef.inmemory.cache.CacheStatusMessage;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class HdfsCacheManagerTest {

  @Test
  public void testBindPortUpdate() throws IOException {
    HdfsTaskSelectionPolicy selector = mock(HdfsTaskSelectionPolicy.class);
    HdfsCacheManager manager = new HdfsCacheManager(selector);

    RunningTask task = TestUtils.mockRunningTask("a", "hosta");

    manager.addRunningTask(task);
    try {
      manager.getCacheAddress(task.getId());
      fail("Expected IOException when port not assigned");
    } catch (IOException e) {
      // Expected exception
    }

    manager.handleUpdate(task.getId(), TestUtils.cacheStatusMessage(18001));
    assertEquals("hosta:18001", manager.getCacheAddress(task.getId()));

    manager.handleUpdate(task.getId(), TestUtils.cacheStatusMessage(18001));
    assertEquals("hosta:18001", manager.getCacheAddress(task.getId()));
  }
}
