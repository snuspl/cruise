package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;

public class CacheNode {
  private final RunningTask task;
  private final String address;

  public CacheNode(RunningTask task, int port) {
    this.task = task;
    this.address = getCacheHost(this.task) + ":" + port;
  }

  public RunningTask getTask() {
    return task;
  }

  public String getTaskId() {
    return task.getId();
  }

  public String getAddress() {
    return address;
  }

  /**
   * Gets the Cache server's address
   * This will return the node's address as defined by Wake, based on its network interfaces,
   * so it can be contacted remotely. This means that you will not see localhost/127.0.0.1
   * as the hostname even on local deployments.
   * @See com.microsoft.wake.remote.NetUtils.getLocalAddress()
   */
  private static String getCacheHost(final RunningTask task) {
    return task.getActiveContext().getEvaluatorDescriptor()
            .getNodeDescriptor().getInetSocketAddress().getHostString();
  }
}
