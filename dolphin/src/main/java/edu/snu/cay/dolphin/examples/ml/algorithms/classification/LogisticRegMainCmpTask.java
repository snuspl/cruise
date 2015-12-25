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

import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.core.WorkloadQuota;
import edu.snu.cay.dolphin.examples.ml.data.LinearModel;
import edu.snu.cay.dolphin.examples.ml.data.LogisticRegSummary;
import edu.snu.cay.dolphin.examples.ml.data.Row;
import edu.snu.cay.dolphin.examples.ml.data.RowDataType;
import edu.snu.cay.dolphin.examples.ml.loss.Loss;
import edu.snu.cay.dolphin.examples.ml.parameters.StepSize;
import edu.snu.cay.dolphin.examples.ml.regularization.Regularization;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.commons.lang.math.LongRange;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogisticRegMainCmpTask extends UserComputeTask
    implements DataReduceSender<LogisticRegSummary>, DataBroadcastReceiver<LinearModel> {

  private static final Logger LOG = Logger.getLogger(LogisticRegMainCmpTask.class.getName());

  private double stepSize;
  private final Loss loss;
  private final Regularization regularization;
  private final String dataType;
  private final WorkloadQuota workloadQuota;
  private final MemoryStore memoryStore;
  private LinearModel model;
  private int posNum = 0;
  private int negNum = 0;

  @Inject
  public LogisticRegMainCmpTask(@Parameter(StepSize.class) final double stepSize,
                                final Loss loss,
                                final Regularization regularization,
                                @Parameter(RowDataType.class) final String dataType,
                                final WorkloadQuota workloadQuota,
                                final MemoryStore memoryStore) {
    this.stepSize = stepSize;
    this.loss = loss;
    this.regularization = regularization;
    this.dataType = dataType;
    this.workloadQuota = workloadQuota;
    this.memoryStore = memoryStore;
  }

  @Override
  public final void run(final int iteration) {

    // measure accuracy
    posNum = 0;
    negNum = 0;

    final Set<LongRange> ranges = workloadQuota.get(dataType);

    if (ranges == null) {
      LOG.log(Level.INFO, "No {0} type data assigned to this task", dataType);
      return;
    }

    for (final LongRange range : ranges) {
      final Map<?, Row> rows = memoryStore.getElasticStore().getRange(dataType,
          range.getMinimumLong(), range.getMaximumLong());
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
        final Vector gradient = loss.gradient(input, model.predict(input), output).plus(regularization.gradient(model));
        model.setParameters(model.getParameters().minus(gradient.times(stepSize)));
      }
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
