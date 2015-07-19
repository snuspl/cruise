package edu.snu.cay.services.em.driver;

import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;

/**
 * Manager class for keeping track of partitions registered by evaluators.
 * TODO: Currently does not check whether ranges are disjoint or not.
 * TODO: Currently does not try to merge contiguous ranges.
 */
@DriverSide
public final class PartitionManager {

  private final Map<String, Map<String, TreeSet<LongRange>>> mapIdKeyRange;

  private Comparator<LongRange> longRangeComparator = new Comparator<LongRange>() {
    @Override
    public int compare(LongRange o1, LongRange o2) {
      return (int) (o1.getMinimumLong() - o2.getMinimumLong());
    }
  };

  @Inject
  private PartitionManager() {
    this.mapIdKeyRange = new HashMap<>();
  }

  public void registerPartition(final String evalId,
                                final String key, final long unitStartId, final long unitEndId) {
    registerPartition(evalId, key, new LongRange(unitStartId, unitEndId));
  }

  public void registerPartition(final String evalId, final String key, final LongRange idRange) {
    if (!mapIdKeyRange.containsKey(evalId)) {
      mapIdKeyRange.put(evalId, new HashMap<String, TreeSet<LongRange>>());
    }

    final Map<String, TreeSet<LongRange>> mapKeyRange = mapIdKeyRange.get(evalId);
    if (!mapKeyRange.containsKey(key)) {
      mapKeyRange.put(key, new TreeSet<>(longRangeComparator));
    }

    mapKeyRange.get(key).add(idRange);
  }

  public Set<LongRange> getRangeSet(final String evalId, final String key) {
    if (!mapIdKeyRange.containsKey(evalId)) {
      return null;
    }

    final Map<String, TreeSet<LongRange>> mapKeyRange = mapIdKeyRange.get(evalId);
    if (!mapKeyRange.containsKey(key)) {
      return null;
    }

    return mapKeyRange.get(key);
  }

  public boolean remove(final String evalId, final String key, final LongRange longRange) {
    if (!mapIdKeyRange.containsKey(evalId)) {
      return false;
    }

    final Map<String, TreeSet<LongRange>> mapKeyRange = mapIdKeyRange.get(evalId);
    if (!mapKeyRange.containsKey(key)) {
      return false;
    }

    return mapKeyRange.get(key).remove(longRange);
  }
}
