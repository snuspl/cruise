package org.apache.reef.inmemory.common.hdfs;

import com.microsoft.reef.util.Optional;
import org.apache.reef.inmemory.common.CacheClearMessage;
import org.apache.reef.inmemory.common.write.BlockAllocateMessage;

import java.io.Serializable;

/**
 * Parent object for messages sent from Driver to Task. Each message will only
 * hold a single child message.
 */
public final class HdfsDriverTaskMessage implements Serializable {

  private Optional<HdfsBlockMessage> hdfsBlockMessage = Optional.empty();
  private Optional<CacheClearMessage> clearMessage = Optional.empty();
  private Optional<BlockAllocateMessage> allocateMessage = Optional.empty();

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

  public static HdfsDriverTaskMessage allocateMessage(BlockAllocateMessage allocateMessage) {
    HdfsDriverTaskMessage msg = new HdfsDriverTaskMessage();
    msg.allocateMessage = Optional.of(allocateMessage);
    return msg;
  }

  public Optional<HdfsBlockMessage> getHdfsBlockMessage() {
    return hdfsBlockMessage;
  }

  public Optional<CacheClearMessage> getClearMessage() {
    return clearMessage;
  }

  public Optional<BlockAllocateMessage> getAllocateMessage() {
    return allocateMessage;
  }
}
