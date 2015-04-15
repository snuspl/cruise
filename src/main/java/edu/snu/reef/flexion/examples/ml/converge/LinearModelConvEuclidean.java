package edu.snu.reef.flexion.examples.ml.converge;

import edu.snu.reef.flexion.examples.ml.data.EuclideanDistance;
import edu.snu.reef.flexion.examples.ml.data.LinearModel;
import edu.snu.reef.flexion.examples.ml.parameters.ConvergenceThreshold;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Default implementation of LinearModelConvCond
 * Algorithm converges when its model is changed less than
 * a certain threshold in terms of Euclidean distance after an iteration
 */
public class LinearModelConvEuclidean implements  LinearModelConvCond {

  LinearModel oldModel;
  final double convergenceThreshold;
  final EuclideanDistance euclideanDistance;

  @Inject
  public LinearModelConvEuclidean(
      final EuclideanDistance euclideanDistance,
      @Parameter(ConvergenceThreshold.class) final double convergenceThreshold
  ) {
    this.euclideanDistance = euclideanDistance;
    this.convergenceThreshold = convergenceThreshold;
  }

  @Override
  public final boolean checkConvergence(LinearModel model) {

    if (oldModel == null) {
      oldModel = new LinearModel(model.getParameters().clone());
      return false;
    } else {
      return distance(oldModel.getParameters(), model.getParameters()) < convergenceThreshold;
    }
  }

  public final double distance(Vector v1, Vector v2) {
    return euclideanDistance.distance(v1, v2);
  }

}
