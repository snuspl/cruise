package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;

/**
 * Encapsulates cache node information
 */
public class CacheNode {
  private final RunningTask task;
  private final String address;

  public CacheNode(final RunningTask task,
                   final int port) {
    this.task = task;
    this.address = getCacheHost(this.task) + ":" + port;
  }

  public String getTaskId() {
    return task.getId();
  }

  public String getAddress() {
    return address;
  }

  public void send(final byte[] msg) {
    task.send(msg);
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
