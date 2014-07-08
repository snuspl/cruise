package org.apache.reef.inmemory.task.hdfs;

import com.microsoft.reef.util.Optional;
import org.apache.reef.inmemory.common.CacheClearMessage;

import java.io.Serializable;

/**
 * Parent object for messages sent from driver to Task. Each message will only
 * hold a single child message.
 */
public final class HdfsMessage implements Serializable {

  private final Optional<HdfsBlockMessage> blockMessage;
  private final Optional<CacheClearMessage> clearMessage;

  public HdfsMessage(HdfsBlockMessage blockMessage) {
    this.blockMessage = Optional.of(blockMessage);
    this.clearMessage = Optional.empty();
  }

  public HdfsMessage(CacheClearMessage clearMessage) {
    this.blockMessage = Optional.empty();
    this.clearMessage = Optional.of(clearMessage);
  }

  public Optional<HdfsBlockMessage> getBlockMessage() {
    return blockMessage;
  }

  public Optional<CacheClearMessage> getClearMessage() {
    return clearMessage;
  }
}
