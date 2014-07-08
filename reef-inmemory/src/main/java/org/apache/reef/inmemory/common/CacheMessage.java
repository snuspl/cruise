package org.apache.reef.inmemory.common;

import com.microsoft.reef.util.Optional;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;

import java.io.Serializable;

/**
 * Parent object for messages sent from driver to Task. Each message will only
 * hold a single child message.
 */
public final class CacheMessage implements Serializable {

  private Optional<HdfsBlockMessage> hdfsBlockMessage = Optional.empty();
  private Optional<CacheClearMessage> clearMessage = Optional.empty();

  public CacheMessage() {
  }

  public static CacheMessage hdfsBlockMessage(HdfsBlockMessage hdfsBlockMessage) {
    CacheMessage msg = new CacheMessage();
    msg.hdfsBlockMessage = Optional.of(hdfsBlockMessage);
    return msg;
  }

  public static CacheMessage clearMessage(CacheClearMessage clearMessage) {
    CacheMessage msg = new CacheMessage();
    msg.clearMessage = Optional.of(clearMessage);
    return msg;
  }

  public Optional<HdfsBlockMessage> getHdfsBlockMessage() {
    return hdfsBlockMessage;
  }

  public Optional<CacheClearMessage> getClearMessage() {
    return clearMessage;
  }
}
