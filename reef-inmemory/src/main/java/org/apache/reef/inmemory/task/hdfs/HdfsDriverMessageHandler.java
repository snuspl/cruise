package org.apache.reef.inmemory.task.hdfs;

import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsDriverTaskMessage;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.common.write.BlockAllocateMessage;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.DriverMessageHandler;
import org.apache.reef.inmemory.task.InMemoryCache;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles messages from the Driver, related to cache loading and management.
 * Cache loading logic requires HDFS-specific messages.
 */
public final class HdfsDriverMessageHandler implements DriverMessageHandler {

  private static final Logger LOG = Logger.getLogger(HdfsDriverMessageHandler.class.getName());
  private static final ObjectSerializableCodec<HdfsDriverTaskMessage> HDFS_CODEC = new ObjectSerializableCodec<>();

  private final InMemoryCache cache;

  @Inject
  public HdfsDriverMessageHandler(final InMemoryCache cache) {
    this.cache = cache;
  }

  @Override
  public void onNext(DriverMessage driverMessage) {
    if (driverMessage.get().isPresent()) {
      final HdfsDriverTaskMessage msg = HDFS_CODEC.decode(driverMessage.get().get());
      if (msg.getHdfsBlockMessage().isPresent()) {
        LOG.log(Level.INFO, "Received load block msg");
        final HdfsBlockMessage blockMsg = msg.getHdfsBlockMessage().get();
        final HdfsBlockLoader loader = new HdfsBlockLoader(blockMsg.getBlockId(), blockMsg.getLocations(), blockMsg.isPin(), cache.getLoadingBufferSize());

        try {
          cache.load(loader);
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Could not load block", e);
        }

      } else if (msg.getClearMessage().isPresent()) {
        LOG.log(Level.INFO, "Received cache clear msg");
        cache.clear();
      } else if (msg.getAllocateMessage().isPresent()) {
        final BlockAllocateMessage allocateMessage = msg.getAllocateMessage().get();
        final BlockId blockId = allocateMessage.getBlockId();
        final Action action = allocateMessage.getAction();
        cache.allocate(blockId, action);
      }
    }
  }
}