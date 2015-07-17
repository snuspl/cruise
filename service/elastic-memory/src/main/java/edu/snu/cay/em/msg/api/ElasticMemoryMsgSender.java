package edu.snu.cay.em.msg.api;

import edu.snu.cay.em.avro.UnitIdPair;
import edu.snu.cay.em.msg.impl.ElasticMemoryMsgSenderImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Interface for sending AvroElasticMemoryMessages to the driver and evaluators.
 */
@DefaultImplementation(ElasticMemoryMsgSenderImpl.class)
public interface ElasticMemoryMsgSender {

  /**
   * Send a CtrlMsg that tells the Evaluator specified with {@code destId} to
   * send its {@code dataClassName} data to the Evaluator specified with
   * {@code targetEvalId}.
   */
  void sendCtrlMsg(final String destId,
                   final String dataClassName,
                   final String targetEvalId);

  /**
   * Send a DataMsg containing {@code unitIdPairList} to the Evaluator
   * named {@code destId}. The data key is {@code dataClassName}.
   */
  void sendDataMsg(final String destId,
                   final String dataClassName,
                   final List<UnitIdPair> unitIdPairList);
}
