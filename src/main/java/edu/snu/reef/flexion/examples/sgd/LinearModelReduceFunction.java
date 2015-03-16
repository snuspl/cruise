package edu.snu.reef.flexion.examples.sgd;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.flexion.examples.sgd.data.LinearModel;
import edu.snu.reef.flexion.examples.sgd.data.Model;
import org.apache.mahout.math.Vector;

import javax.inject.Inject;

public final class LinearModelReduceFunction implements Reduce.ReduceFunction<Model> {
  @Inject
  public LinearModelReduceFunction() {
  }

  @Override
  public final Model apply(Iterable<Model> elements) {
    Vector parameter = null;
    int count = 0;

    for (Model elem : elements) {
      if (!(elem instanceof LinearModel)) {
        continue;
      }

      LinearModel linearModel = (LinearModel) elem;

      count++;
      if (parameter == null) {
        parameter = linearModel.getParameters();
      } else {
        parameter = parameter.plus(linearModel.getParameters());
      }
    }

    parameter = parameter.times((double)1 / count);
    return new LinearModel(parameter);

  }
}
  
