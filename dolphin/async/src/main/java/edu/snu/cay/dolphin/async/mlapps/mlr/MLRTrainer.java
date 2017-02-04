/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.ModelAccessor;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.TrainingDataProvider;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.utils.ThreadUtils;
import edu.snu.cay.utils.Tuple3;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.mlapps.mlr.MLRParameters.*;

/**
 * {@link Trainer} class for the MLRREEF application.
 * Uses {@code numClasses} model vectors to determine which class each data instance belongs to.
 * The model vector that outputs the highest dot product value is declared as that data instance's prediction.
 */
final class MLRTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(MLRTrainer.class.getName());

  /**
   * ParameterWorker object used to interact with the parameter server.
   */
  private final ParameterWorker<Integer, Vector, Vector> parameterWorker;

  /**
   * Number of possible classes for a data instance.
   */
  private final int numClasses;

  /**
   * Number of features of each model partition.
   */
  private final int numFeaturesPerPartition;

  /**
   * Number of model partitions for each class.
   */
  private final int numPartitionsPerClass;

  /**
   * Number of training data instances to be processed per mini-batch.
   */
  private final int miniBatchSize;

  /**
   * Size of each step taken during gradient descent.
   */
  private double stepSize;

  /**
   * L2 regularization constant.
   */
  private final double lambda;

  /**
   * Object for creating {@link Vector} instances.
   */
  private final VectorFactory vectorFactory;

  /**
   * Preserves the model parameters that are pulled from server, in order to compute the gradients to push.
   */
  private final Vector[] oldParams;

  /**
   * A list from 0 to {@code numClasses * numPartitionsPerClass} that will be used during {@link #pullModels()}.
   */
  private List<Integer> classPartitionIndices;

  /**
   * The step size drops by this rate.
   */
  private final double decayRate;

  /**
   * The step size drops after every {@code decayPeriod} iterations pass.
   */
  private final int decayPeriod;

  private final MemoryStore<Long> memoryStore;
  private final TrainingDataProvider<Long, MLRData> trainingDataProvider;

  /**
   * Executes the Trainer threads.
   */
  private final ExecutorService executor;

  /**
   * Number of Trainer threads that train concurrently.
   */
  private final int numTrainerThreads;

  /**
   * Allows to access and update the latest model.
   */
  private final ModelAccessor<MLRModel> modelAccessor;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;
  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  @Inject
  private MLRTrainer(final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                     @Parameter(NumClasses.class) final int numClasses,
                     @Parameter(NumFeatures.class) final int numFeatures,
                     @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                     @Parameter(InitialStepSize.class) final double initStepSize,
                     @Parameter(Lambda.class) final double lambda,
                     @Parameter(DecayRate.class) final double decayRate,
                     @Parameter(DecayPeriod.class) final int decayPeriod,
                     @Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                     @Parameter(Parameters.NumTrainerThreads.class) final int numTrainerThreads,
                     final ModelAccessor<MLRModel> modelAccessor,
                     final MemoryStore<Long> memoryStore,
                     final TrainingDataProvider<Long, MLRData> trainingDataProvider,
                     final MetricsMsgSender<WorkerMetrics> metricsMsgSender,
                     final VectorFactory vectorFactory) {
    this.parameterWorker = parameterWorker;
    this.numClasses = numClasses;
    this.numFeaturesPerPartition = numFeaturesPerPartition;
    if (numFeatures % numFeaturesPerPartition != 0) {
      throw new RuntimeException("Uneven model partitions");
    }
    this.numPartitionsPerClass = numFeatures / numFeaturesPerPartition;
    this.miniBatchSize = miniBatchSize;
    this.stepSize = initStepSize;
    this.lambda = lambda;
    this.vectorFactory = vectorFactory;
    this.oldParams = new Vector[numClasses];

    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }
    this.metricsMsgSender = metricsMsgSender;
    this.modelAccessor = modelAccessor;
    this.memoryStore = memoryStore;
    this.trainingDataProvider = trainingDataProvider;

    this.numTrainerThreads = numTrainerThreads;
    this.executor = Executors.newFixedThreadPool(numTrainerThreads);

    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  /**
   * Parse the input dataset, initialize a few data structures, and wait for other workers.
   */
  @Override
  public void initialize() {
    classPartitionIndices = new ArrayList<>(numClasses * numPartitionsPerClass);
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      for (int partitionIndex = 0; partitionIndex < numPartitionsPerClass; ++partitionIndex) {
        // 0 ~ (numPartitionsPerClass - 1) is for class 0
        // numPartitionsPerClass ~ (2 * numPartitionsPerClass - 1) is for class 1
        // and so on
        classPartitionIndices.add(classIndex * numPartitionsPerClass + partitionIndex);
      }
    }

    LOG.log(Level.INFO, "Number of Trainer threads = {0}", numTrainerThreads);
    LOG.log(Level.INFO, "Step size = {0}", stepSize);
    LOG.log(Level.INFO, "Number of instances per mini-batch = {0}", miniBatchSize);
    LOG.log(Level.INFO, "Total number of keys = {0}", classPartitionIndices.size());
  }

  @Override
  public void run(final int iteration) {
    final long epochStartTime = System.currentTimeMillis();

    // Record the number of EM data blocks at the beginning of this iteration
    // to filter out stale metrics for optimization
    final int numEMBlocks = memoryStore.getNumBlocks();

    int miniBatchIdx = 0;
    int numTotalInstancesProcessed = 0;
    final List<MLRData> totalInstancesProcessed = new LinkedList<>();

    Map<Long, MLRData> nextTrainingData = trainingDataProvider.getNextTrainingData();

    while (!nextTrainingData.isEmpty()) {
      final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

      final BlockingQueue<MLRData> instances = new ArrayBlockingQueue<>(miniBatchSize);
      instances.addAll(nextTrainingData.values());
      final int numInstancesToProcess = instances.size();

      resetTracers();
      final long miniBatchStartTime = System.currentTimeMillis();

      // pull data when mini-batch is started
      pullModels();

      // collects the results (new models here) computed by multiple threads
      final List<Future<MLRModel>> futures = new ArrayList<>(numTrainerThreads);
      try {
        computeTracer.startTimer();

        // Threads drain multiple instances from shared queue, as many as nInstances / (nThreads)^2.
        // This way we can mitigate the slowdown from straggler threads.
        final int drainSize = Math.min(instances.size() / numTrainerThreads / numTrainerThreads, 1);

        for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
          final Future<MLRModel> future = executor.submit(() -> {
            final List<MLRData> drainedInstances = new ArrayList<>(drainSize);
            final MLRModel model = modelAccessor.getModel()
                .orElseThrow(() -> new RuntimeException("Model was not initialized properly"));

            int count = 0;
            while (true) {
              final int numDrained = instances.drainTo(drainedInstances, drainSize);
              if (numDrained == 0) {
                break;
              }

              drainedInstances.forEach(instance -> updateModel(instance, model));
              drainedInstances.clear();
              count += numDrained;
            }
            latch.countDown();
            LOG.log(Level.INFO, "{0} has computed {1} instances",
                new Object[] {Thread.currentThread().getName(), count});
            return model;
          });
          futures.add(future);
        }
        latch.await();
        computeTracer.recordTime(numInstancesToProcess);
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Exception occurred.", e);
        throw new RuntimeException(e);
      }

      final List<MLRModel> newModels = ThreadUtils.retrieveResults(futures);
      final Vector[] gradients = aggregateGradient(newModels);

      // push gradients
      pushAndResetGradients(gradients);

      // update the instances processed so far
      numTotalInstancesProcessed += numInstancesToProcess;
      totalInstancesProcessed.addAll(nextTrainingData.values());

      // load the set of training data instances to process in the next mini-batch
      nextTrainingData = trainingDataProvider.getNextTrainingData();

      final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;
      final WorkerMetrics miniBatchMetric =
          buildMiniBatchMetric(iteration, miniBatchIdx, numInstancesToProcess, miniBatchElapsedTime);
      LOG.log(Level.INFO, "WorkerMetrics {0}", miniBatchMetric);
      sendMetrics(miniBatchMetric);

      miniBatchIdx++;
    }

    if (!(decayRate == 1) && iteration % decayPeriod == 0) {
      final double prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} iterations have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }

    LOG.log(Level.INFO, "Pull model to compute loss value");
    pullModels();

    final MLRModel model = modelAccessor.getModel()
        .orElseThrow(() -> new RuntimeException("Model was not initialized properly"));

    LOG.log(Level.INFO, "Start computing loss value");
    final Tuple3<Double, Double, Double> lossRegLossAccuracy = computeLoss(totalInstancesProcessed, model);

    final double epochElapsedTime = (System.currentTimeMillis() - epochStartTime) / 1000.0D;
    final double sampleLoss = lossRegLossAccuracy.getFirst();
    final double regLoss = lossRegLossAccuracy.getSecond();
    final double accuracy = lossRegLossAccuracy.getThird();
    final WorkerMetrics epochMetric =
        buildEpochMetric(iteration, miniBatchIdx, numEMBlocks,
            numTotalInstancesProcessed, sampleLoss, regLoss, accuracy, epochElapsedTime);

    LOG.log(Level.INFO, "WorkerMetrics {0}", epochMetric);
    sendMetrics(epochMetric);
  }

  /**
   * Pull models one last time and perform validation.
   */
  @Override
  public void cleanup() {
    executor.shutdown();
  }

  /**
   * Pull up-to-date model parameters from server, which become accessible via {@link ModelAccessor#getModel()}.
   */
  private void pullModels() {
    pullTracer.startTimer();
    final List<Vector> partitions = parameterWorker.pull(classPartitionIndices);
    pullTracer.recordTime(partitions.size());

    computeTracer.startTimer();
    final Vector[] newParams = new Vector[numClasses];
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      // 0 ~ (numPartitionsPerClass - 1) is for class 0
      // numPartitionsPerClass ~ (2 * numPartitionsPerClass - 1) is for class 1
      // and so on
      final List<Vector> partialModelsForThisClass =
          partitions.subList(classIndex * numPartitionsPerClass, (classIndex + 1) * numPartitionsPerClass);

      // concat partitions into one long vector
      oldParams[classIndex] = vectorFactory.concatDense(partialModelsForThisClass);
      newParams[classIndex] = oldParams[classIndex].copy();
    }

    modelAccessor.resetModel(new MLRModel(newParams));
    computeTracer.recordTime(0);
  }

  /**
   * Processes one training data instance and update the intermediate model.
   * @param instance training data instance
   * @param model up-to-date model
   */
  private void updateModel(final MLRData instance, final MLRModel model) {
    final Vector feature = instance.getFeature();
    final Vector[] params = model.getParams();
    final int label = instance.getLabel();

    // compute h(x, w) = softmax(x dot w)
    final Vector predictions = predict(feature, params);

    // error = h(x, w) - y, where y_j = 1 (if positive for class j) or 0 (otherwise)
    // instead of allocating a new vector for the error,
    // we use the same object for convenience
    predictions.set(label, predictions.get(label) - 1);

    // gradient_j = -stepSize * error_j * x
    if (lambda != 0) {
      for (int j = 0; j < numClasses; ++j) {
        params[j].axpy(-predictions.get(j) * stepSize, feature);
        params[j].axpy(-stepSize * lambda, params[j]);
      }
    } else {
      for (int j = 0; j < numClasses; ++j) {
        params[j].axpy(-predictions.get(j) * stepSize, feature);
      }
    }
  }

  /**
   * Aggregate the model computed by multiple threads, to get the gradients to push.
   * gradient[j] = sum(param_t[j] - param_0[j]) = sum(param_t[j]) - t * param_0[j],
   * where j is the class index, t is the thread index and param_0 is the parameters pulled at the beginning.
   * @param results list of results (model parameters) computed by trainer threads
   * @return an array of vectors each of which is gradient in a class.
   */
  private Vector[] aggregateGradient(final List<MLRModel> results) {
    final Vector[] gradients = new Vector[numClasses];

    // Multiply the number of threads (t) to weight the old model parameters when getting difference.
    for (int classIdx = 0; classIdx < numClasses; classIdx++) {
      gradients[classIdx] = oldParams[classIdx].scale(-numTrainerThreads);
    }

    // Compute the sum of the model parameters computed by training threads
    for (final MLRModel model : results) {
      final Vector[] params = model.getParams();
      for (int classIdx = 0; classIdx < numClasses; classIdx++) {
        gradients[classIdx].addi(params[classIdx]);
      }
    }
    return gradients;
  }

  /**
   * Push the gradients to parameter server.
   * @param gradients an array of vectors each of which is gradient in a class.
   */
  private void pushAndResetGradients(final Vector[] gradients) {
    for (int classIndex = 0; classIndex < numClasses; classIndex++) {
      computeTracer.startTimer();
      final Vector gradient = gradients[classIndex];
      computeTracer.recordTime(0);

      pushTracer.startTimer();
      for (int partitionIndex = 0; partitionIndex < numPartitionsPerClass; ++partitionIndex) {
        final int partitionStart = partitionIndex * numFeaturesPerPartition;
        final int partitionEnd = (partitionIndex + 1) * numFeaturesPerPartition;
        parameterWorker.push(classIndex * numPartitionsPerClass + partitionIndex,
            gradient.slice(partitionStart, partitionEnd));
      }
      pushTracer.recordTime(numPartitionsPerClass);
    }
  }

  /**
   * Compute the loss value using the current models and given data instances.
   * May take long, so do not call frequently.
   */
  private Tuple3<Double, Double, Double> computeLoss(final List<MLRData> data, final MLRModel model) {
    final Vector[] params = model.getParams();

    double loss = 0;
    int correctPredictions = 0;

    for (final MLRData entry : data) {
      final Vector feature = entry.getFeature();
      final int label = entry.getLabel(); final Vector predictions = predict(feature, params);
      final int prediction = max(predictions).getFirst();

      if (label == prediction) {
        ++correctPredictions;
      }

      loss += -Math.log(predictions.get(label));
    }

    double regLoss = 0;
    if (lambda != 0) {
      // skip this part entirely if lambda is zero, to avoid regularization operation overheads
      for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
        final Vector perClassParams = params[classIndex];
        double l2norm = 0;
        for (int vectorIndex = 0; vectorIndex < perClassParams.length(); ++vectorIndex) {
          l2norm += perClassParams.get(vectorIndex) * perClassParams.get(vectorIndex);
        }
        regLoss += l2norm * lambda / 2;
      }
    }
    regLoss /= numClasses;

    return new Tuple3<>(loss, regLoss, (double) correctPredictions / data.size());
  }

  /**
   * Compute the probability vector of the given data instance, represented by {@code features}.
   */
  private Vector predict(final Vector features, final Vector[] params) {
    final double[] predict = new double[numClasses];
    for (int classIndex = 0; classIndex < numClasses; ++classIndex) {
      predict[classIndex] = params[classIndex].dot(features);
    }
    return softmax(vectorFactory.createDense(predict));
  }

  private static Vector softmax(final Vector vector) {
    // prevent overflow during exponential operations
    // https://lingpipe-blog.com/2009/06/25/log-sum-of-exponentials/
    final double logSumExp = logSumExp(vector);
    for (int index = 0; index < vector.length(); ++index) {
      vector.set(index, Math.max(Math.min(1 - 1e-12, Math.exp(vector.get(index) - logSumExp)), 1e-12));
    }
    return vector;
  }

  /**
   * Returns {@code log(sum_i(exp(vector.get(i)))}, while avoiding overflow.
   */
  private static double logSumExp(final Vector vector) {
    final double max = max(vector).getSecond();
    double sumExp = 0;
    for (int index = 0; index < vector.length(); ++index) {
      sumExp += Math.exp(vector.get(index) - max);
    }
    return max + Math.log(sumExp);
  }

  /**
   * Find the largest value in {@code vector} and return its index and the value itself together.
   */
  private static Pair<Integer, Double> max(final Vector vector) {
    double maxValue = vector.get(0);
    int maxIndex = 0;
    for (int index = 1; index < vector.length(); ++index) {
      final double value = vector.get(index);
      if (value > maxValue) {
        maxValue = value;
        maxIndex = index;
      }
    }
    return new Pair<>(maxIndex, maxValue);
  }

  private void resetTracers() {
    pushTracer.resetTrace();
    pullTracer.resetTrace();
    computeTracer.resetTrace();
  }

  private void sendMetrics(final WorkerMetrics workerMetrics) {
    LOG.log(Level.FINE, "Sending WorkerMetrics {0}", new Object[]{workerMetrics});

    metricsMsgSender.send(workerMetrics);
  }

  private WorkerMetrics buildMiniBatchMetric(final int iteration, final int miniBatchIdx,
                                             final int numProcessedDataItemCount, final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.DVT, numProcessedDataItemCount / elapsedTime);

    return WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder()
            .setData(appMetricMap)
            .build())
        .setEpochIdx(iteration)
        .setMiniBatchSize(miniBatchSize)
        .setMiniBatchIdx(miniBatchIdx)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .setTotalCompTime(computeTracer.totalElapsedTime())
        .setTotalPullTime(pullTracer.totalElapsedTime())
        .setAvgPullTime(pullTracer.avgTimePerElem())
        .setTotalPushTime(pushTracer.totalElapsedTime())
        .setAvgPushTime(pushTracer.avgTimePerElem())
        .setParameterWorkerMetrics(parameterWorker.buildParameterWorkerMetrics())
        .build();
  }

  private WorkerMetrics buildEpochMetric(final int iteration, final int numMiniBatchForEpoch,
                                         final int numDataBlocks, final int numProcessedDataItemCount,
                                         final double sampleLoss, final double regLoss, final double accuracy,
                                         final double elapsedTime) {
    final Map<CharSequence, Double> appMetricMap = new HashMap<>();
    appMetricMap.put(MetricKeys.SAMPLE_LOSS_SUM, sampleLoss);
    appMetricMap.put(MetricKeys.REG_LOSS_AVG, regLoss);
    appMetricMap.put(MetricKeys.ACCURACY, accuracy);
    parameterWorker.buildParameterWorkerMetrics(); // clear ParameterWorker metrics

    return WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder()
            .setData(appMetricMap)
            .build())
        .setEpochIdx(iteration)
        .setMiniBatchSize(miniBatchSize)
        .setNumMiniBatchForEpoch(numMiniBatchForEpoch)
        .setNumDataBlocks(numDataBlocks)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .build();
  }
}
