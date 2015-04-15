package edu.snu.reef.flexion.examples.ml.converge;

import edu.snu.reef.flexion.examples.ml.data.LinearModel;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for a check function that decides whether
 * a linear classification/regression algorithm has converged or not.
 */
@DefaultImplementation(LinearModelConvEuclidean.class)
public interface LinearModelConvCond {

  /**
   * Check convergence conditions.
   */
  public boolean checkConvergence(LinearModel model);
}