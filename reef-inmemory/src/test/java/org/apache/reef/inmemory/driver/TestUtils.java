package org.apache.reef.inmemory.driver;

import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.reef.inmemory.task.CacheStatusMessage;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestUtils {
  public static RunningTask mockRunningTask(final String id, final String hostString) {
    final RunningTask runningTask = mock(RunningTask.class);
    final ActiveContext activeContext = mock(ActiveContext.class);
    final EvaluatorDescriptor evaluatorDescriptor = mock(EvaluatorDescriptor.class);
    final NodeDescriptor nodeDescriptor = mock(NodeDescriptor.class);
    // Mockito can't mock the final method getHostString(), so using real object
    final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostString, 0);

    doReturn(id).when(runningTask).getId();
    doReturn(activeContext).when(runningTask).getActiveContext();
    doReturn(evaluatorDescriptor).when(activeContext).getEvaluatorDescriptor();
    doReturn(nodeDescriptor).when(evaluatorDescriptor).getNodeDescriptor();
    doReturn(inetSocketAddress).when(nodeDescriptor).getInetSocketAddress();

    return runningTask;
  }

  public static CacheStatusMessage cacheStatusMessage(final int port) {
    return new CacheStatusMessage(port);
  }
}
