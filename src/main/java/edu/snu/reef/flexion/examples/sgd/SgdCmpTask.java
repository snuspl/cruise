package edu.snu.reef.flexion.examples.sgd;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.examples.sgd.data.Example;
import edu.snu.reef.flexion.examples.sgd.data.Model;
import edu.snu.reef.flexion.examples.sgd.data.SgdDataParser;
import edu.snu.reef.flexion.examples.sgd.loss.Loss;
import edu.snu.reef.flexion.examples.sgd.parameters.Alpha;
import edu.snu.reef.flexion.examples.sgd.regularization.Regularization;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceSender;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

public class SgdCmpTask extends UserComputeTask<List<Example>> implements IDataBroadcastReceiver<Model>, IDataReduceSender<Model> {
  
  private double alpha;
  private final Loss loss;
  private final Regularization regularization;
  
  private List<Example> examples;
  private Model model;
  
  @Inject
  public SgdCmpTask(@Parameter(Alpha.class) final double alpha,
                    final Loss loss,
                    final Regularization regularization) {
    this.alpha = alpha;
    this.loss = loss;
    this.regularization = regularization;
  }
  
  @Override
  public final void run(int iteration, List<Example> data) {
    if (iteration == 0) {
      examples = data;
    }

    
    for (final Example example : examples) {
      double label = example.getLabel();
      Vector point = example.getPoint();
      double gradient = loss.gradient(model.predict(point), label);
      Vector parameters = model.getParameters();
      model.setParameters(parameters.plus(point.times(-alpha * gradient)));
      model.setParameters(parameters.plus(regularization.derivative(model).times(-alpha)));
      
      alpha *= 0.9;
    }
    
  }
  
  @Override
  public final void receiveBroadcastData(Model model) {
    this.model = model;
  }
  
  @Override
  public final Model sendReduceData(int iteration) {
    return model;
    
  }
  
  @Override
  public final Class<? extends Codec> getReduceCodecClass() {
    return LinearModelCodec.class;
  }
  
  @Override
  public final Class<? extends Reduce.ReduceFunction<Model>> getReduceFunctionClass() {
    return LinearModelReduceFunction.class;
    
  }
  

  @Override
  public final Class<? extends DataParser<List<Example>>> getDataParserClass() {
    return SgdDataParser.class;
    
  }
}
