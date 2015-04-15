package edu.snu.reef.flexion.examples.ml.algorithms.regression;

import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.ParseException;
import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.examples.ml.data.LinearModel;
import edu.snu.reef.flexion.examples.ml.data.Row;
import edu.snu.reef.flexion.examples.ml.data.SGDSummary;
import edu.snu.reef.flexion.examples.ml.loss.Loss;
import edu.snu.reef.flexion.examples.ml.parameters.StepSize;
import edu.snu.reef.flexion.examples.ml.regularization.Regularization;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceSender;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

public class LinearRegCmpTask extends UserComputeTask
    implements DataReduceSender<SGDSummary>, DataBroadcastReceiver<LinearModel> {

  private double stepSize;
  private final Loss loss;
  private final Regularization regularization;

  private DataParser<List<Row>> dataParser;
  private List<Row> rows;
  private LinearModel model;
  private double lossSum = 0;

  @Inject
  public LinearRegCmpTask(@Parameter(StepSize.class) final double stepSize,
                          final Loss loss,
                          final Regularization regularization,
                          DataParser<List<Row>> dataParser) {
    this.stepSize = stepSize;
    this.loss = loss;
    this.regularization = regularization;
    this.dataParser = dataParser;
  }

  @Override
  public void initialize() throws ParseException {
    rows = dataParser.get();
  }

  @Override
  public final void run(int iteration) {

    // measure loss
    lossSum = 0;
    for (final Row row : rows) {
      final double output = row.getOutput();
      final double predict = model.predict(row.getFeature());
      lossSum += loss.loss(predict, output);
    }

    // optimize
    for (final Row row : rows) {
      final double output = row.getOutput();
      final Vector input = row.getFeature();
      final Vector gradient = loss.gradient(input, model.predict(input), output).plus(regularization.gradient(model));
      model.setParameters(model.getParameters().minus(gradient.times(stepSize)));
    }
  }

  @Override
  public final void receiveBroadcastData(LinearModel model) {
    this.model = model;
  }

  @Override
  public SGDSummary sendReduceData(int iteration) {
    return new SGDSummary(this.model, 1, this.lossSum);
  }
}
