package edu.snu.cay.services.em.evaluator.api;

import org.apache.reef.annotations.audience.EvaluatorSide;

/**
 * Evaluator-side interface of MemoryStore, which provides two SubMemoryStores: local and elastic.
 */
@EvaluatorSide
public interface MemoryStore {

  /**
   * {@code SubMemoryStore} that stores local data that should not be moved to other evaluators.
   */
  SubMemoryStore getLocalStore();

  /**
   * {@code SubMemoryStore} that stores movable data that migrated around evaluators for job optimization.
   */
  SubMemoryStore getElasticStore();
}
