package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test simple random policy
 */
public final class HdfsRandomTaskSelectionPolicyTest {

  private LocatedBlock getMockBlock() {
    return mock(LocatedBlock.class);
  }

  private List<RunningTask> getMockTasks(int size) {
    ArrayList<RunningTask> mockTasks = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      mockTasks.add(mock(RunningTask.class));
    }
    return mockTasks;
  }

  @Test
  public void testLessThanDefault() {
    HdfsRandomTaskSelectionPolicy policy = new HdfsRandomTaskSelectionPolicy(3);
    List<RunningTask> tasks = getMockTasks(1);
    List<RunningTask> selected = policy.select(getMockBlock(), tasks);
    assertTrue(tasks.containsAll(selected));
    assertTrue(tasks.containsAll(selected));
    assertEquals(1, selected.size());
  }

  @Test
  public void testIsShuffled() {
    HdfsRandomTaskSelectionPolicy policy = new HdfsRandomTaskSelectionPolicy(3);
    List<RunningTask> tasks = getMockTasks(100);
    for (int i = 0; i < 10; i++) { // With high probability
      List<RunningTask> selected = policy.select(getMockBlock(), tasks);
      assertTrue(tasks.containsAll(selected));
      for (int j = 0; j < 3; j++) {
        if (tasks.get(j) != selected.get(j)) {
          return;
        }
      }
    }
    assertTrue("Selected tasks do not appear to be shuffled", false);
  }
}
