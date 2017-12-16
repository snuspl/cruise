/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.spl.cruise.ps.core.worker;

import edu.snu.spl.cruise.common.metric.avro.Metrics;
import edu.snu.spl.cruise.ps.CruisePSParameters;
import edu.snu.spl.cruise.ps.ModelEvalAnsMsg;
import edu.snu.spl.cruise.ps.metric.avro.CruiseWorkerMetrics;
import edu.snu.spl.cruise.ps.metric.avro.WorkerMetricsType;
import edu.snu.spl.cruise.services.et.evaluator.api.Table;
import edu.snu.spl.cruise.services.et.evaluator.api.TableAccessor;
import edu.snu.spl.cruise.services.et.exceptions.TableNotExistException;
import edu.snu.spl.cruise.services.et.metric.MetricCollector;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A component for evaluating a trained model.
 */
public final class ModelEvaluator {
  private static final Logger LOG = Logger.getLogger(ModelEvaluator.class.getName());

  private final InjectionFuture<TableAccessor> tableAccessorFuture;
  private final InjectionFuture<MetricCollector> metricCollectorFuture;
  private final InjectionFuture<WorkerSideMsgSender> msgSenderFuture;

  private final String modelTableId;
  private final String inputTableId;

  private final ResettingCountDownLatch latch = new ResettingCountDownLatch(1);
  private volatile boolean doNext = true;

  @Inject
  private ModelEvaluator(final InjectionFuture<TableAccessor> tableAccessorFuture,
                         final InjectionFuture<WorkerSideMsgSender> msgSenderFuture,
                         final InjectionFuture<MetricCollector> metricCollectorFuture,
                         @Parameter(CruisePSParameters.ModelTableId.class) final String modelTableId,
                         @Parameter(CruisePSParameters.InputTableId.class) final String inputTableId) {
    this.tableAccessorFuture = tableAccessorFuture;
    this.modelTableId = modelTableId;
    this.inputTableId = inputTableId;
    this.metricCollectorFuture = metricCollectorFuture;
    this.msgSenderFuture = msgSenderFuture;
  }

  /**
   * Evaluate all checkpointed models.
   */
  void evaluate(final Trainer trainer, final List testData) {
    int modelCount = 0;
    while (askMasterForCheckpointedModel()) {

      LOG.log(Level.INFO, "Evaluate a {0}th model", modelCount++);
      final Table modelTable;
      final Table inputTable;
      try {
        modelTable = tableAccessorFuture.get().getTable(modelTableId);
        inputTable = tableAccessorFuture.get().getTable(inputTableId);
      } catch (TableNotExistException e) {
        throw new RuntimeException(e);
      }

      final Collection trainingData = inputTable.getLocalTablet().getDataMap().values();

      final Map<CharSequence, Double> objValue = trainer.evaluateModel(trainingData, testData, modelTable);

      final CruiseWorkerMetrics metrics = CruiseWorkerMetrics.newBuilder()
          .setType(WorkerMetricsType.ModelEvalMetrics)
          .setObjValue(Metrics.newBuilder().setData(objValue).build())
          .build();

      // send metric to master
      metricCollectorFuture.get().addCustomMetric(metrics);
      metricCollectorFuture.get().flush();
    }

    LOG.log(Level.INFO, "Finish evaluating models");
  }

  /**
   * Tell master that it's ready to evaluate the next model.
   * And wait master's response.
   * @return True if there exists a next model table to evaluate
   */
  private boolean askMasterForCheckpointedModel() {
    LOG.log(Level.INFO, "Ask master.");
    // send message to master
    try {
      msgSenderFuture.get().sendModelEvalAskMsg();
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }

    // wait on latch
    latch.awaitAndReset(1);

    return doNext;
  }

  /**
   * A response from master about {@link #askMasterForCheckpointedModel()}.
   */
  void onMasterMsg(final ModelEvalAnsMsg msg) {
    this.doNext = msg.getDoNext();

    // release latch
    latch.countDown();
  }
}
