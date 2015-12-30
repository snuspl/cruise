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
package edu.snu.cay.dolphin.examples.ml.algorithms.classification;

import edu.snu.cay.common.math.vector.Vector;
import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.core.WorkloadPartition;
import edu.snu.cay.dolphin.examples.ml.data.LinearModel;
import edu.snu.cay.dolphin.examples.ml.data.LogisticRegSummary;
import edu.snu.cay.dolphin.examples.ml.data.Row;
import edu.snu.cay.dolphin.examples.ml.data.RowDataType;
import edu.snu.cay.dolphin.examples.ml.loss.Loss;
import edu.snu.cay.dolphin.examples.ml.parameters.StepSize;
import edu.snu.cay.dolphin.examples.ml.regularization.Regularization;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceSender;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;

public class LogisticRegMainCmpTask extends UserComputeTask
    implements DataReduceSender<LogisticRegSummary>, DataBroadcastReceiver<LinearModel> {

  private double stepSize;
  private final Loss loss;
  private final Regularization regularization;
  private final String dataType;
  private final WorkloadPartition workloadPartition;
  private LinearModel model;
  private int posNum = 0;
  private int negNum = 0;

  @Inject
  public LogisticRegMainCmpTask(@Parameter(StepSize.class) final double stepSize,
                                final Loss loss,
                                final Regularization regularization,
                                @Parameter(RowDataType.class) final String dataType,
                                final WorkloadPartition workloadPartition) {
    this.stepSize = stepSize;
    this.loss = loss;
    this.regularization = regularization;
    this.dataType = dataType;
    this.workloadPartition = workloadPartition;
  }

  @Override
  public final void run(final int iteration) {

    // measure accuracy
    posNum = 0;
    negNum = 0;

    final Map<?, Row> rows = workloadPartition.getAllData(dataType);

    for (final Row row : rows.values()) {
      final double output = row.getOutput();
      final double predict = model.predict(row.getFeature());
      if (output * predict > 0) {
        posNum++;
      } else {
        negNum++;
      }
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
  public LogisticRegSummary sendReduceData(final int iteration) {
    return new LogisticRegSummary(this.model, 1, posNum, negNum);
  }
}
