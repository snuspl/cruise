package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.task.BlockId;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public final class CacheUpdates implements Serializable {

  private final List<Failure> failures = new LinkedList<>();
  private final List<BlockId> removals = new LinkedList<>();

  public void addFailure(final BlockId blockId, final Exception exception) {
    failures.add(new Failure(blockId, exception));
  }

  public void addRemoval(final BlockId blockId) {
    removals.add(blockId);
  }

  public List<Failure> getFailures() {
    return failures;
  }

  public List<BlockId> getRemovals() {
    return removals;
  }

  public final static class Failure implements Serializable {
    private final BlockId blockId;
    private final Exception exception;

    private Failure(BlockId blockId, Exception exception) {
      this.blockId = blockId;
      this.exception = exception;
    }

    public BlockId getBlockId() {
      return blockId;
    }

    public Exception getException() {
      return exception;
    }
  }
}
