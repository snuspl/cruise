package edu.snu.reef.em.utils;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;

import javax.inject.Inject;

public final class ElasticMemoryMessageBroadcastHandler
    extends SingleMessageBroadcastHandler<AvroElasticMemoryMessage> {

  @Inject
  public ElasticMemoryMessageBroadcastHandler() {
  }
}
