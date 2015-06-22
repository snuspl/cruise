package edu.snu.reef.dolphin.core.metric;

import javax.inject.Inject;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metric tracker for the number of garbage collections and the elapsed time for garbage collections
 */
public final class MetricTrackerGC implements MetricTracker {

  /**
   * key for the GC count measure (the number of garbage collection)
   */
  public final static String KEY_METRIC_GC_COUNT = "METRIC_GC_COUNT";

  /**
   * key for the GC time measure (elapsed time for garbage collection)
   */
  public final static String KEY_METRIC_GC_TIME = "METRIC_GC_TIME";

  /**
   * total number of garbage collections that have occurred when starting to track measures
   */
  private int startGCCount = 0;

  /**
   * approximate accumulated garbage collection elapsed time in milliseconds when starting to track measures
   */
  private long startGCTime = 0;

  /**
   * This class is instantiated by TANG
   *
   * Constructor for the Garbage Collector tracker
   */
  @Inject
  public MetricTrackerGC() {
  }

  public void start() {
    startGCCount = 0;
    startGCTime = 0;
    for(final GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      final long localCount = garbageCollectorMXBean.getCollectionCount();
      final long localTime = garbageCollectorMXBean.getCollectionTime();
      if (localCount > 0) {
        startGCCount += localCount;
      }
      if (localTime > 0) {
        startGCTime = localTime;
      }
    }
  }

  public Map<String, Double> stop() {
    long endGCCount = 0;
    long endGCTime = 0;
    for(final GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      final long localCount = garbageCollectorMXBean.getCollectionCount();
      final long localTime = garbageCollectorMXBean.getCollectionTime();
      if (localCount > 0) {
        endGCCount += localCount;
      }
      if (localTime > 0) {
        endGCTime = localTime;
      }
    }

    final Map<String, Double> result = new TreeMap<>();
    result.put(KEY_METRIC_GC_COUNT, (double)(endGCCount - startGCCount));
    result.put(KEY_METRIC_GC_TIME, (double)(endGCTime - startGCTime));
    return result;
  }

  public void close() {
  }
}
