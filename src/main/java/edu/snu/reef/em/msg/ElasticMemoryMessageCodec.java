package edu.snu.reef.em.msg;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import edu.snu.reef.em.utils.AvroUtils;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

public class ElasticMemoryMessageCodec
    implements Codec<AvroElasticMemoryMessage> {

  @Inject
  public ElasticMemoryMessageCodec() {
  }

  @Override
  public byte[] encode(final AvroElasticMemoryMessage msg) {
    return AvroUtils.toBytes(msg, AvroElasticMemoryMessage.class);
  }

  @Override
  public AvroElasticMemoryMessage decode(final byte[] data) {
    return AvroUtils.fromBytes(data, AvroElasticMemoryMessage.class);
  }
}
