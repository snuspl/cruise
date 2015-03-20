package org.apache.reef.inmemory.common;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Block updates at the Task:
 * - failures : blocks that failed during load
 * - removals : blocks that were removed due to eviction or clear
 * - additions : blocks that were added after write
 */
public final class CacheUpdates implements Serializable {

  private final List<Failure> failures = new LinkedList<>();
  private final List<BlockId> removals = new LinkedList<>();
  private final List<Addition> additions = new LinkedList<>();

  public void addFailure(final BlockId blockId, final Throwable exception) {
    failures.add(new Failure(blockId, exception));
  }

  public void addRemoval(final BlockId blockId) {
    removals.add(blockId);
  }

  public void addAddition(final BlockId blockId, final long amount) {
    additions.add(new Addition(blockId, amount));
  }

  public List<Failure> getFailures() {
    return failures;
  }

  public List<BlockId> getRemovals() {
    return removals;
  }

  public List<Addition> getAddition() {
    return additions;
  }

  public final static class Failure implements Serializable {
    private final BlockId blockId;
    private final Throwable throwable;

    private Failure(final BlockId blockId, final Throwable throwable) {
      this.blockId = blockId;
      this.throwable = throwable;
    }

    public BlockId getBlockId() {
      return blockId;
    }

    public Throwable getThrowable() {
      return throwable;
    }
  }

  public final static class Addition implements Serializable {
    private final BlockId blockId;
    private final long length; // For the last block, the amount of written data could be smaller than the block size

    private Addition(final BlockId blockId, final long length) {
      this.blockId = blockId;
      this.length = length;
    }

    public BlockId getBlockId() {
      return blockId;
    }

    public long getLength() {
      return length;
    }
  }
}
