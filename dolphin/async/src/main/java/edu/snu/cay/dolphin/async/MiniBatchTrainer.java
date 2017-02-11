package edu.snu.cay.dolphin.async;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yunseong on 2/12/17.
 */
public interface MiniBatchTrainer<D> {
  void initialize();

  void cleanup();

  void run(int idx, AtomicBoolean aborted);

  void runBatch(final Collection<D> batchData);

  void evaluateModel(final Collection<D> epochData);
}
