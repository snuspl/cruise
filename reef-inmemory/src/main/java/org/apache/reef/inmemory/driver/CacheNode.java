package org.apache.reef.inmemory.driver;

import com.microsoft.reef.driver.task.RunningTask;
import org.apache.reef.inmemory.common.CacheStatistics;

/**
 * Encapsulates task node information
 */
public class CacheNode {
  private final RunningTask task;
  private final String address;
  private final String rack;
  private final int memory;
  private CacheStatistics latestStatistics;

  public CacheNode(final RunningTask task,
                   final int port) {
    this.task = task;
    this.address = getCacheHost(this.task) + ":" + port;
    this.rack = getCacheRack(this.task);
    this.memory = this.task.getActiveContext().getEvaluatorDescriptor().getMemory();
  }

  public String getTaskId() {
    return task.getId();
  }

  public String getAddress() {
    return address;
  }

  public String getRack() {
    return rack;
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

  private static String getCacheRack(final RunningTask task) {
    return task.getActiveContext().getEvaluatorDescriptor()
            .getNodeDescriptor().getRackDescriptor().getName();
  }

  public CacheStatistics getLatestStatistics() {
    return latestStatistics;
  }

  public void setLatestStatistics(CacheStatistics latestStatistics) {
    this.latestStatistics = latestStatistics;
  }

  /**
   * Return the memory size of the allocated container
   */
  public int getMemory() {
    return memory;
  }
}
