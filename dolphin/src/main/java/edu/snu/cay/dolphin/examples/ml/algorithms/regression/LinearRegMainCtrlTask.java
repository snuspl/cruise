/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.examples.ml.algorithms.regression;

import edu.snu.cay.dolphin.examples.ml.data.LinearModel;
import edu.snu.cay.dolphin.examples.ml.parameters.Dimension;
import edu.snu.cay.dolphin.examples.ml.parameters.MaxIterations;
import edu.snu.cay.dolphin.core.UserControllerTask;
import edu.snu.cay.dolphin.examples.ml.converge.LinearModelConvCond;
import edu.snu.cay.dolphin.examples.ml.data.LinearRegSummary;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceReceiver;
import org.apache.mahout.math.DenseVector;
import org.apache.reef.io.data.output.OutputStreamProvider;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LinearRegMainCtrlTask extends UserControllerTask
    implements DataReduceReceiver<LinearRegSummary>, DataBroadcastSender<LinearModel> {
  private static final Logger LOG = Logger.getLogger(LinearRegMainCtrlTask.class.getName());

  private final LinearModelConvCond convergeCondition;
  private final int maxIter;
  private double lossSum;
  private LinearModel model;
  private final OutputStreamProvider outputStreamProvider;

  @Inject
  public LinearRegMainCtrlTask(final OutputStreamProvider outputStreamProvider,
                               final LinearModelConvCond convergeCondition,
                               @Parameter(MaxIterations.class) final int maxIter,
                               @Parameter(Dimension.class) final int dimension) {
    this.outputStreamProvider = outputStreamProvider;
    this.convergeCondition = convergeCondition;
    this.maxIter = maxIter;
    this.model = new LinearModel(new DenseVector(dimension + 1));
  }
  
  @Override
  public final void run(final int iteration) {
    LOG.log(Level.INFO, "{0}-th iteration loss sum: {1}, new model: {2}",
        new Object[]{iteration, lossSum, model});
  }
  
  @Override
  public final LinearModel sendBroadcastData(final int iteration) {
    return model;
  }
  
  @Override
  public final boolean isTerminated(final int iteration) {
    return convergeCondition.checkConvergence(model) || iteration >= maxIter;
  }

  @Override
  public void receiveReduceData(final int iteration, final LinearRegSummary sgdSummary) {
    this.lossSum = sgdSummary.getLoss();
    this.model = new LinearModel(sgdSummary.getModel().getParameters()
        .times(1.0 / sgdSummary.getCount()));
  }

  @Override
  public void cleanup() {

    //output the learned model and its accuracy
    try (final DataOutputStream modelStream = outputStreamProvider.create("model");
         final DataOutputStream accuracyStream = outputStreamProvider.create("loss")) {
      modelStream.writeBytes(model.toString());
      accuracyStream.writeBytes(String.valueOf(lossSum));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
