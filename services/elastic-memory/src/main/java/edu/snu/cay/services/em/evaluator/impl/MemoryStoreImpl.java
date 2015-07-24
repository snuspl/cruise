package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.api.SubMemoryStore;
import edu.snu.cay.services.em.trace.HTrace;

import javax.inject.Inject;

public final class MemoryStoreImpl implements MemoryStore {

  private final SubMemoryStore localStore;
  private final SubMemoryStore elasticStore;

  @Inject
  private MemoryStoreImpl(final HTrace hTrace) {
    hTrace.initialize();
    this.localStore = new SubMemoryStoreImpl();
    this.elasticStore = new SubMemoryStoreImpl();
  }

  @Override
  public SubMemoryStore getLocalStore() {
    return this.localStore;
  }

  @Override
  public SubMemoryStore getElasticStore() {
    return this.elasticStore;
  }
}
