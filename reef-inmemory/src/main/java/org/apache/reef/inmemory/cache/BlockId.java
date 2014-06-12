package org.apache.reef.inmemory.cache;

public abstract class BlockId {
  public abstract String getFs();

  // Equals should include unique getFs() String
  public abstract boolean equals(Object other);
  public abstract int hashCode();
}
