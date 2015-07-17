package edu.snu.cay.services.em.msg.api;

import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.msg.impl.ElasticMemoryMsgSenderImpl;
import org.apache.htrace.TraceInfo;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;
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
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendCtrlMsg(final String destId,
                   final String dataClassName,
                   final String targetEvalId,
                   @Nullable final TraceInfo parentTraceInfo);

  /**
   * Send a DataMsg containing {@code unitIdPairList} to the Evaluator
   * named {@code destId}. The data key is {@code dataClassName}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataMsg(final String destId,
                   final String dataClassName,
                   final List<UnitIdPair> unitIdPairList,
                   @Nullable final TraceInfo parentTraceInfo);
}
