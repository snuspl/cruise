package edu.snu.reef.em.driver.impl;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Represents the driver-side message handler for receiving ElasticMemoryMessages.
 */
@DriverSide
public final class ElasticMemoryMsgHandlerDriver implements EventHandler<AvroElasticMemoryMessage> {

  // TODO: This constructor should be declared `private`, but ElasticMemoryImpl
  // needs to directly instantiate this class in order to use NSWrapper and thus
  // we keep this constructor `default`.
  @Inject
  ElasticMemoryMsgHandlerDriver() {
  }

  @Override
  public void onNext(final AvroElasticMemoryMessage msg) {
    throw new RuntimeException("The Driver shouldn't be receiving messages.");
  }
}
