package edu.snu.cay.services.em.evaluator.api;

import edu.snu.cay.services.em.evaluator.impl.EagerPartitionRegister;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for evaluators to notify the driver that they will use a certain partition.
 */
@DefaultImplementation(EagerPartitionRegister.class)
@EvaluatorSide
public interface PartitionRegister {

  void registerPartition(String key, long startId, long endId);
}
