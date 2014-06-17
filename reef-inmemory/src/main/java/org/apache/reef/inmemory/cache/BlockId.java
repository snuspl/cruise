package org.apache.reef.inmemory.cache;

/**
 * An abstract Block ID, to be used to identify blocks cached at each Task.
 */
public abstract class BlockId {
  public abstract String getFs();

  // Equals should include unique getFs() String
  public abstract boolean equals(Object other);
  public abstract int hashCode();
}
