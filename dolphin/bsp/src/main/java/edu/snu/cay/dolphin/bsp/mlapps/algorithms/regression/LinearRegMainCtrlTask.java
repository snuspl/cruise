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
package edu.snu.cay.dolphin.bsp.mlapps.algorithms.regression;

import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.dolphin.bsp.core.UserControllerTask;
import edu.snu.cay.dolphin.bsp.mlapps.converge.LinearModelConvCond;
import edu.snu.cay.dolphin.bsp.mlapps.data.LinearModel;
import edu.snu.cay.dolphin.bsp.mlapps.data.LinearRegSummary;
import edu.snu.cay.dolphin.bsp.mlapps.parameters.Dimension;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataReduceReceiver;
import edu.snu.cay.dolphin.bsp.parameters.MaxIterations;
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
                               final VectorFactory vectorFactory,
                               @Parameter(MaxIterations.class) final int maxIter,
                               @Parameter(Dimension.class) final int dimension) {
    this.outputStreamProvider = outputStreamProvider;
    this.convergeCondition = convergeCondition;
    this.maxIter = maxIter;
    this.model = new LinearModel(vectorFactory.createDenseZeros(dimension + 1));
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
        .scale(1.0 / sgdSummary.getCount()));
  }

  @Override
  public void cleanup() {

    //output the learned model and its accuracy
    try (DataOutputStream modelStream = outputStreamProvider.create("model");
         DataOutputStream accuracyStream = outputStreamProvider.create("loss")) {
      modelStream.writeBytes(model.toString());
      accuracyStream.writeBytes(String.valueOf(lossSum));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
