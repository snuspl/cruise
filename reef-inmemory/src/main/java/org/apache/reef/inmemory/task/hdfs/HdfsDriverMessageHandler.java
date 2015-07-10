package org.apache.reef.inmemory.task.hdfs;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.DeleteBlocksMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsDriverTaskMessage;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.task.DriverMessageHandler;
import org.apache.reef.inmemory.task.InMemoryCache;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

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
  private final EventRecorder record;

  private static final ObjectSerializableCodec<HdfsDriverTaskMessage> HDFS_CODEC = new ObjectSerializableCodec<>();

  private final InMemoryCache cache;

  @Inject
  public HdfsDriverMessageHandler(final InMemoryCache cache,
                                  final EventRecorder recorder) {
    this.cache = cache;
    this.record = recorder;
  }

  @Override
  public void onNext(final DriverMessage driverMessage) {
    if (driverMessage.get().isPresent()) {
      final HdfsDriverTaskMessage msg = HDFS_CODEC.decode(driverMessage.get().get());
      if (msg.getHdfsBlockMessage().isPresent()) {
        LOG.log(Level.INFO, "Received load block msg");
        final HdfsBlockMessage blockMsg = msg.getHdfsBlockMessage().get();
        final HdfsBlockLoader loader = new HdfsBlockLoader(blockMsg.getBlockId(), blockMsg.getBlockInfo(),
                blockMsg.getLocations(), blockMsg.isPin(), cache.getLoadingBufferSize(), record);

        try {
          cache.load(loader);
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Could not load block", e);
        }

      } else if (msg.getClearMessage().isPresent()) {
        LOG.log(Level.INFO, "Received cache clear msg");
        cache.clear();
      } else if (msg.getDeleteMessage().isPresent()) {
        LOG.log(Level.INFO, "Received delete block msg");
        final DeleteBlocksMessage deleteMsg = msg.getDeleteMessage().get();
        for (final BlockId blockId : deleteMsg.getBlockIds()){
          cache.delete(blockId);
        }
      }
    }
  }
}
