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
package edu.snu.cay.dolphin.bsp.examples.ml.algorithms.regression;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.bsp.examples.ml.data.RowDataType;
import edu.snu.cay.dolphin.bsp.examples.ml.data.LinearModel;
import edu.snu.cay.dolphin.bsp.core.UserComputeTask;
import edu.snu.cay.dolphin.bsp.examples.ml.data.Row;
import edu.snu.cay.dolphin.bsp.examples.ml.data.LinearRegSummary;
import edu.snu.cay.dolphin.bsp.examples.ml.loss.Loss;
import edu.snu.cay.dolphin.bsp.examples.ml.parameters.StepSize;
import edu.snu.cay.dolphin.bsp.examples.ml.regularization.Regularization;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;

public class LinearRegMainCmpTask extends UserComputeTask
    implements DataReduceSender<LinearRegSummary>, DataBroadcastReceiver<LinearModel> {

  private double stepSize;
  private final Loss loss;
  private final Regularization regularization;
  private final String dataType;
  private final MemoryStore memoryStore;
  private LinearModel model;
  private double lossSum = 0;

  @Inject
  public LinearRegMainCmpTask(@Parameter(StepSize.class) final double stepSize,
                              final Loss loss,
                              final Regularization regularization,
                              @Parameter(RowDataType.class) final String dataType,
                              final MemoryStore memoryStore) {
    this.stepSize = stepSize;
    this.loss = loss;
    this.regularization = regularization;
    this.dataType = dataType;
    this.memoryStore = memoryStore;
  }

  @Override
  public final void run(final int iteration) {

    // measure loss
    lossSum = 0;
    final Map<?, Row> rows = memoryStore.getAll(dataType);
    for (final Row row : rows.values()) {
      final double output = row.getOutput();
      final double predict = model.predict(row.getFeature());
      lossSum += loss.loss(predict, output);
    }

    // optimize
    for (final Row row : rows.values()) {
      final double output = row.getOutput();
      final Vector input = row.getFeature();
      final Vector gradient = loss.gradient(input, model.predict(input), output).add(regularization.gradient(model));
      model.setParameters(model.getParameters().sub(gradient.scale(stepSize)));
    }
  }

  @Override
  public final void receiveBroadcastData(final int iteration, final LinearModel modelData) {
    this.model = modelData;
  }

  @Override
  public LinearRegSummary sendReduceData(final int iteration) {
    return new LinearRegSummary(this.model, 1, this.lossSum);
  }
}
