package org.apache.reef.inmemory.cache.hdfs;

import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.cache.DriverMessageHandler;
import org.apache.reef.inmemory.cache.InMemoryCache;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class HdfsDriverMessageHandler implements DriverMessageHandler {
  private static final Logger LOG = Logger.getLogger(HdfsDriverMessageHandler.class.getName());
  private static final ObjectSerializableCodec<HdfsMessage> CODEC = new ObjectSerializableCodec<>();

  private final HdfsBlockLoader blockLoader;
  private final InMemoryCache cache;

  @Inject
  public HdfsDriverMessageHandler(HdfsBlockLoader blockLoader,
                                  InMemoryCache cache) {
    this.blockLoader = blockLoader;
    this.cache = cache;
  }

  @Override
  public void onNext(DriverMessage driverMessage) {

    if (driverMessage.get().isPresent()) {
      HdfsMessage msg = CODEC.decode(driverMessage.get().get());
      if (msg.getBlockMessage().isPresent()) {
        LOG.log(Level.INFO, "Received load block msg");
        blockLoader.loadOnMessage(msg.getBlockMessage().get());
      } else if (msg.getClearMessage().isPresent()) {
        LOG.log(Level.INFO, "Received cache clear msg");
        cache.clear();
      }
    }
  }
}
