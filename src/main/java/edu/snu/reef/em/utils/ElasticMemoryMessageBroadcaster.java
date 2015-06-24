package edu.snu.reef.em.utils;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;

import javax.inject.Inject;

/**
 * The SingleMessageBroadcastHandler class can't be used directly with Tang
 * due to its generic type parameter. This class fixes that generic type to
 * AvroElasticMemoryMessage, allowing Tang static configuration and injection.
 */
public final class ElasticMemoryMessageBroadcaster
    extends SingleMessageBroadcastHandler<AvroElasticMemoryMessage> {

  @Inject
  public ElasticMemoryMessageBroadcaster() {
  }
}
