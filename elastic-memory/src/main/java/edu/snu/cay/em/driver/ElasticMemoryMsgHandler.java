package edu.snu.cay.em.driver;

import edu.snu.cay.em.avro.AvroElasticMemoryMessage;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Driver-side message handler.
 * Currently does nothing, but we need this class as a placeholder to
 * instantiate NetworkService.
 */
@DriverSide
final class ElasticMemoryMsgHandler implements EventHandler<Message<AvroElasticMemoryMessage>> {

  @Inject
  private ElasticMemoryMsgHandler() {
  }

  @Override
  public void onNext(final Message<AvroElasticMemoryMessage> msg) {
    // TODO: implement
  }
}
