package org.apache.reef.inmemory.common.hdfs;

import org.apache.reef.util.Optional;
import org.apache.reef.inmemory.common.CacheClearMessage;

import java.io.Serializable;

/**
 * Parent object for messages sent from Driver to Task. Each message will only
 * hold a single child message.
 */
public final class HdfsDriverTaskMessage implements Serializable {

  private Optional<HdfsBlockMessage> hdfsBlockMessage = Optional.empty();
  private Optional<CacheClearMessage> clearMessage = Optional.empty();

  public static HdfsDriverTaskMessage hdfsBlockMessage(HdfsBlockMessage hdfsBlockMessage) {
    HdfsDriverTaskMessage msg = new HdfsDriverTaskMessage();
    msg.hdfsBlockMessage = Optional.of(hdfsBlockMessage);
    return msg;
  }

  public static HdfsDriverTaskMessage clearMessage(CacheClearMessage clearMessage) {
    HdfsDriverTaskMessage msg = new HdfsDriverTaskMessage();
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
