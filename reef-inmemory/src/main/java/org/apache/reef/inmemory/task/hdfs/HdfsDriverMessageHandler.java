package org.apache.reef.inmemory.task.hdfs;

import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.wake.EStage;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsDriverTaskMessage;
import org.apache.reef.inmemory.task.BlockLoader;
import org.apache.reef.inmemory.task.DriverMessageHandler;
import org.apache.reef.inmemory.task.InMemoryCache;

import javax.inject.Inject;
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
  private final EStage<BlockLoader> loadingStage;

  @Inject
  public HdfsDriverMessageHandler(final InMemoryCache cache,
                                  final EStage<BlockLoader> loadingStage) {
    this.cache = cache;
    this.loadingStage = loadingStage;
  }

  @Override
  public void onNext(DriverMessage driverMessage) {
    if (driverMessage.get().isPresent()) {
      final HdfsDriverTaskMessage msg = HDFS_CODEC.decode(driverMessage.get().get());
      if (msg.getHdfsBlockMessage().isPresent()) {
        LOG.log(Level.INFO, "Received load block msg");
        final HdfsBlockMessage blockMsg = msg.getHdfsBlockMessage().get();

        // TODO: pass request to InMemoryCache. IMC can check if block already exists, call executeLoad if not.
        final HdfsBlockLoader loader = new HdfsBlockLoader(blockMsg.getBlockId(), blockMsg.getLocations());
        loadingStage.onNext(loader);
      } else if (msg.getClearMessage().isPresent()) {
        LOG.log(Level.INFO, "Received cache clear msg");
        cache.clear();
      }
    }
  }
}
