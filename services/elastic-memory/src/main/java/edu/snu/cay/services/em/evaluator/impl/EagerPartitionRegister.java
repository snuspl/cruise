package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.PartitionRegister;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.trace.HTrace;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;

/**
 * Sends a partition register message to the driver as soon as it is called.
 */
@EvaluatorSide
public final class EagerPartitionRegister implements PartitionRegister {
  private static final String REGISTER_PARTITION = "registerPartition";

  private final ElasticMemoryMsgSender sender;

  @Inject
  private EagerPartitionRegister(final ElasticMemoryMsgSender sender,
                                 final HTrace htrace) {
    htrace.initialize();
    this.sender = sender;
  }

  @Override
  public void registerPartition(final String key, final long startId, final long endId) {
    final TraceScope traceScope = Trace.startSpan(REGISTER_PARTITION);
    try {
      sender.sendRegisMsg(key, startId, endId, TraceInfo.fromSpan(traceScope.getSpan()));
    } finally {
      traceScope.close();
    }
  }
}
