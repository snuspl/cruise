package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.task.BlockId;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Block updates at the Task:
 * - failures: blocks that failed during load
 * - removals: blocks that were removed due to eviction or clear
 */
public final class CacheUpdates implements Serializable {

  private final List<Failure> failures = new LinkedList<>();
  private final List<BlockId> removals = new LinkedList<>();
  private final List<BlockId> written = new LinkedList<>();

  public void addFailure(final BlockId blockId, final Throwable exception) {
    failures.add(new Failure(blockId, exception));
  }

  public void addRemoval(final BlockId blockId) {
    removals.add(blockId);
  }

  public void addWritten(final BlockId blockId) {
    written.add(blockId);
  }

  public List<Failure> getFailures() {
    return failures;
  }

  public List<BlockId> getRemovals() {
    return removals;
  }

  public List<BlockId> getWritten() {
    return written;
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
}
