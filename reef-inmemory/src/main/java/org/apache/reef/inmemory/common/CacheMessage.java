package org.apache.reef.inmemory.common;

import com.microsoft.reef.util.Optional;
import org.apache.reef.inmemory.common.CacheClearMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;

import java.io.Serializable;

/**
 * Parent object for messages sent from driver to Task. Each message will only
 * hold a single child message.
 */
public final class CacheMessage implements Serializable {

  private final Optional<HdfsBlockMessage> blockMessage;
  private final Optional<CacheClearMessage> clearMessage;

  public CacheMessage(HdfsBlockMessage blockMessage) {
    this.blockMessage = Optional.of(blockMessage);
    this.clearMessage = Optional.empty();
  }

  public CacheMessage(CacheClearMessage clearMessage) {
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
