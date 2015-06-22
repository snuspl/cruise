package edu.snu.reef.dolphin.core.metric;


import java.util.Map;

/**
 * Interface that metric trackers implement
 */
public interface MetricTracker extends AutoCloseable {

  /**
   * start tracking metrics
   */
  public void start();

  /**
   * stop tracking metrics and return currently tracked metrics
   * @return key and value of metrics
   */
  public Map<String, Double> stop();

  /**
   * close the metric tracker
   */
  public void close();
}
