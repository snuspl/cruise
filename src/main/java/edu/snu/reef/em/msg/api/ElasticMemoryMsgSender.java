package edu.snu.reef.em.msg.api;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import edu.snu.reef.em.avro.UnitIdPair;
import edu.snu.reef.em.msg.impl.ElasticMemoryMsgSenderImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Interface for sending AvroElasticMemoryMessages to the driver and evaluators.
 */
@DefaultImplementation(ElasticMemoryMsgSenderImpl.class)
public interface ElasticMemoryMsgSender {

  void send(final String destId, final AvroElasticMemoryMessage msg);

  void sendDataMsg(final String destId,
                   final String dataClassName,
                   final List<UnitIdPair> unitIdPairList);
}