package edu.snu.reef.flexion.examples.ml.algorithms.classification;

import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.examples.ml.converge.LinearModelConvCond;
import edu.snu.reef.flexion.examples.ml.data.LinearModel;
import edu.snu.reef.flexion.examples.ml.data.LogisticRegSummary;
import edu.snu.reef.flexion.examples.ml.parameters.Dimension;
import edu.snu.reef.flexion.examples.ml.parameters.MaxIterations;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceReceiver;
import org.apache.mahout.math.DenseVector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogisticRegCtrlTask extends UserControllerTask
    implements DataReduceReceiver<LogisticRegSummary>, DataBroadcastSender<LinearModel> {

  private final static Logger LOG = Logger.getLogger(LogisticRegCtrlTask.class.getName());

  private final LinearModelConvCond convergeCondition;
  private final int maxIter;
  private double accuracy;
  private LinearModel model;


  @Inject
  public LogisticRegCtrlTask(final LinearModelConvCond convergeCondition,
                             @Parameter(MaxIterations.class) final int maxIter,
                             @Parameter(Dimension.class) final int dimension) {
    this.convergeCondition = convergeCondition;
    this.maxIter = maxIter;
    this.model = new LinearModel(new DenseVector(dimension+1));

  }
  
  @Override
  public final void run(int iteration) {
    LOG.log(Level.INFO, "{0}-th iteration accuracy: {1}%, new model: {2}", new Object[] { iteration, accuracy*100, model });
  }
  
  @Override
  public final LinearModel sendBroadcastData(int iteration) {
    return model;
  }
  
  @Override
  public final boolean isTerminated(int iteration) {
    return convergeCondition.checkConvergence(model) || iteration > maxIter;
  }

  @Override
  public void receiveReduceData(LogisticRegSummary summary) {
    this.accuracy = ((double) summary.getPosNum()) / (summary.getPosNum()+summary.getNegNum());
    this.model = new LinearModel(summary.getModel().getParameters().times(1.0/summary.getCount()));
  }
}
