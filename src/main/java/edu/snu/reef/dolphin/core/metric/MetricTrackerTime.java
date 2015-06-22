package edu.snu.reef.dolphin.core.metric;


import javax.inject.Inject;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metric tracker for wall-clock time
 */
public final class MetricTrackerTime implements MetricTracker {

  /**
   * key for the Wall-clock time measure
   */
  public final static String KEY_METRIC_WALL_CLOCK_TIME = "METRIC_WALL_CLOCK_TIME";

  /**
   * elapsed time when starting to track measures
   */
  public long startTime = 0;

  /**
   * This class is instantiated by TANG
   *
   * Constructor for the wall-clock time tracker
   */
  @Inject
  public MetricTrackerTime(){
  }

  public void start() {
    startTime = System.currentTimeMillis();
  }

  public Map<String, Double> stop() {
    final long endTime = System.currentTimeMillis();
    final Map<String, Double> result = new TreeMap<>();
    result.put(KEY_METRIC_WALL_CLOCK_TIME, (double)(endTime - startTime));
    return result;
  }

  public void close() {
  }
}
