package org.apache.reef.inmemory.cache.hdfs;

import com.microsoft.reef.util.Optional;
import org.apache.reef.inmemory.cache.CacheClearMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockMessage;

import java.io.Serializable;

public class HdfsMessage implements Serializable {

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
