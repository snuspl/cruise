package org.apache.reef.elastic.memory.driver;

/**
 * A tuple of a partition and an integer index
 */
public class NumberedPartition implements Comparable<NumberedPartition> {
  private final Partition partition;
  private final int index;

  public NumberedPartition(final Partition partition, final int index) {
    if (partition == null) {
      throw new IllegalArgumentException("state cannot be null");
    }
    this.partition = partition;
    this.index = index;
  }

  public Partition getState() {
    return partition;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return "Partition-" + index;
  }

  @Override
  public int compareTo(NumberedPartition o) {
    if (this.index == o.index)
      return 0;
    if (this.index < o.index)
      return -1;
    else
      return 1;
  }
}
