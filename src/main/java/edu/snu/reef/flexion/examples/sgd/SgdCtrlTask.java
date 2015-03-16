package edu.snu.reef.flexion.examples.sgd;

import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.examples.sgd.data.LinearModel;
import edu.snu.reef.flexion.examples.sgd.data.Model;
import edu.snu.reef.flexion.examples.sgd.parameters.Dimension;
import edu.snu.reef.flexion.examples.sgd.parameters.MaxIterations;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceReceiver;
import org.apache.mahout.math.DenseVector;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SgdCtrlTask extends UserControllerTask implements IDataReduceReceiver<Model>, IDataBroadcastSender<Model> {
  private final static Logger LOG = Logger.getLogger(SgdCtrlTask.class.getName());
  
  private final int maxIter;
  private final int dimension;
  private Model model;
  
  
  @Inject
  public SgdCtrlTask(@Parameter(MaxIterations.class) final int maxIter,
                     @Parameter(Dimension.class) final int dimension) {
    this.maxIter = maxIter;
    this.dimension = dimension;
    this.model = new LinearModel(new DenseVector(dimension + 1)); 
    
  }
  
  @Override
  public final void run(int iteration) {
    if (iteration == 0) {
      return;
    }
    
    LOG.log(Level.INFO, "{0}-th iteration new model: {1}", new Object[] { iteration, model }); 
  }
  
  @Override
  public final Model sendBroadcastData(int iteration) {
    return model;
  }

  @Override
  public final Class<? extends Codec> getBroadcastCodecClass() {
    return LinearModelCodec.class;
  }
  
  @Override
  public final void receiveReduceData(Model data) {
    model = data;
  }
  
  @Override
  public final boolean isTerminated(int iteration) {
    return iteration > maxIter;
    
  }
}
