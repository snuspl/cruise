package org.apache.reef.inmemory.driver;

import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.RackDescriptor;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.common.CacheUpdates;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestUtils {
  public static RunningTask mockRunningTask(final String id, final String hostString) {
    final RunningTask runningTask = mock(RunningTask.class);
    final ActiveContext activeContext = mock(ActiveContext.class);
    final EvaluatorDescriptor evaluatorDescriptor = mock(EvaluatorDescriptor.class);
    final NodeDescriptor nodeDescriptor = mock(NodeDescriptor.class);
    final RackDescriptor rackDescriptor = mock(RackDescriptor.class);
    // Mockito can't mock the final method getHostString(), so using real object
    final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostString, 0);

    doReturn(id).when(runningTask).getId();
    doReturn(activeContext).when(runningTask).getActiveContext();
    doReturn(evaluatorDescriptor).when(activeContext).getEvaluatorDescriptor();
    doReturn(nodeDescriptor).when(evaluatorDescriptor).getNodeDescriptor();
    doReturn(rackDescriptor).when(nodeDescriptor).getRackDescriptor();
    doReturn("/test-rack").when(rackDescriptor).getName();
    doReturn(inetSocketAddress).when(nodeDescriptor).getInetSocketAddress();

    return runningTask;
  }

  public static CacheStatusMessage cacheStatusMessage(final int port) {
    return new CacheStatusMessage(new CacheStatistics(), new CacheUpdates(), port);
  }

  public static CacheManager cacheManager() {
    return new CacheManagerImpl(mock(EvaluatorRequestor.class), "test", 0, 0, 0, 0, 0, 60, "FINE", false, "", 0, "");
  }
}
