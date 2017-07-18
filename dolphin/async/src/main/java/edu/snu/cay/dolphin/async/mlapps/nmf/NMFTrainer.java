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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import com.google.common.collect.Sets;
import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.utils.CatchableExecutors;
import edu.snu.cay.utils.ThreadUtils;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static edu.snu.cay.dolphin.async.mlapps.nmf.NMFParameters.*;

/**
 * Trainer for non-negative matrix factorization via SGD.
 *
 * Assumes that indices in {@link NMFData} are one-based.
 */
final class NMFTrainer implements Trainer<NMFData> {

  private static final Logger LOG = Logger.getLogger(NMFTrainer.class.getName());

  private final ModelAccessor<Integer, Vector, Vector> modelAccessor;
  private final VectorFactory vectorFactory;
  private final int rank;
  private float stepSize;
  private final float lambda;

  private final boolean printMatrices;
  private final NMFModelGenerator modelGenerator;

  /**
   * The step size drops by this rate.
   */
  private final float decayRate;

  /**
   * The step size drops after every {@code decayPeriod} epochs pass.
   */
  private final int decayPeriod;

  /**
   * Executes the Trainer threads.
   */
  private final ExecutorService executor;

  /**
   * Number of Trainer threads that train concurrently.
   */
  private final int numTrainerThreads;

  private final TrainingDataProvider<NMFData> trainingDataProvider;

  @Inject
  private NMFTrainer(final ModelAccessor<Integer, Vector, Vector> modelAccessor,
                     final VectorFactory vectorFactory,
                     @Parameter(Rank.class) final int rank,
                     @Parameter(StepSize.class) final float stepSize,
                     @Parameter(Lambda.class) final float lambda,
                     @Parameter(DecayRate.class) final float decayRate,
                     @Parameter(DecayPeriod.class) final int decayPeriod,
                     @Parameter(NumTotalMiniBatches.class) final int numTotalMiniBatches,
                     @Parameter(PrintMatrices.class) final boolean printMatrices,
                     @Parameter(NumTrainerThreads.class) final int numTrainerThreads,
                     final NMFModelGenerator modelGenerator,
                     final TrainingDataProvider<NMFData> trainingDataProvider) {
    this.modelAccessor = modelAccessor;
    this.vectorFactory = vectorFactory;
    this.rank = rank;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }
    this.printMatrices = printMatrices;
    this.modelGenerator = modelGenerator;
    this.trainingDataProvider = trainingDataProvider;

    this.numTrainerThreads = numTrainerThreads;
    this.executor = CatchableExecutors.newFixedThreadPool(numTrainerThreads);

    LOG.log(Level.INFO, "Number of Trainer threads = {0}", numTrainerThreads);
    LOG.log(Level.INFO, "Step size = {0}", stepSize);
    LOG.log(Level.INFO, "Number of total mini-batches in an epoch = {0}", numTotalMiniBatches);
  }

  @Override
  public void initGlobalSettings() {
  }

  @Override
  public void runMiniBatch(final Collection<NMFData> miniBatchTrainingData) {
    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    final BlockingQueue<NMFData> instances = new ArrayBlockingQueue<>(miniBatchTrainingData.size());
    instances.addAll(miniBatchTrainingData);
    
    // pull data when mini-batch is started
    final List<Integer> keys = getKeys(instances);
    final NMFModel model = pullModels(keys);

    // collect gradients computed in each thread
    final List<Future<Map<Integer, Vector>>> futures = new ArrayList<>(numTrainerThreads);
    try {
      // Threads drain multiple instances from shared queue, as many as nInstances / (nThreads)^2.
      // This way we can mitigate the slowdown from straggler threads.
      final int drainSize = Math.max(instances.size() / numTrainerThreads / numTrainerThreads, 1);

      for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
        final Future<Map<Integer, Vector>> future = executor.submit(() -> {
          final List<NMFData> drainedInstances = new ArrayList<>(drainSize);
          final Map<Integer, Vector> threadRGradient = new HashMap<>();

          int count = 0;
          while (true) {
            final int numDrained = instances.drainTo(drainedInstances, drainSize);
            if (numDrained == 0) {
              break;
            }

            drainedInstances.forEach(instance -> updateGradient(instance, model, threadRGradient));
            drainedInstances.clear();
            count += numDrained;
          }
          latch.countDown();
          LOG.log(Level.INFO, "{0} has computed {1} instances",
              new Object[] {Thread.currentThread().getName(), count});
          return threadRGradient;
        });
        futures.add(future);
      }
      latch.await();
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Exception occurred.", e);
      throw new RuntimeException(e);
    }

    final List<Map<Integer, Vector>> totalRGradients = ThreadUtils.retrieveResults(futures);
    final Map<Integer, Vector> gradients = aggregateGradient(totalRGradients);

    // push gradients
    pushAndResetGradients(gradients);
  }

  @Override
  public EpochResult onEpochFinished(final Collection<NMFData> epochTrainingData,
                                     final Collection<NMFData> testData,
                                     final int epochIdx) {
    LOG.log(Level.INFO, "Pull model to compute loss value");
    final NMFModel model = pullModels(getKeys(epochTrainingData));

    LOG.log(Level.INFO, "Start computing loss value");
    final float trainingLoss = computeLoss(epochTrainingData, model);

    if (decayRate != 1 && (epochIdx + 1) % decayPeriod == 0) {
      final float prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} epochs have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }

    return buildEpochResult(trainingLoss);
  }

  @Override
  public void cleanup() {
    // print generated matrices
    if (!printMatrices) {
      return;
    }
    // print L matrix
    final Collection<NMFData> workload = trainingDataProvider.getEpochData();

    final StringBuilder lsb = new StringBuilder();
    for (final NMFData datum : workload) {
      lsb.append(String.format("L(%d, *):", datum.getRowIndex()));
      for (final VectorEntry valueEntry : datum.getVector()) {
        lsb.append(' ');
        lsb.append(valueEntry.value());
      }
      lsb.append('\n');
    }
    LOG.log(Level.INFO, lsb.toString());

    // print transposed R matrix
    final NMFModel model = pullModels(getKeys(workload));
    final StringBuilder rsb = new StringBuilder();
    for (final Map.Entry<Integer, Vector> entry : model.getRMatrix().entrySet()) {
      rsb.append(String.format("R(*, %d):", entry.getKey()));
      for (final VectorEntry valueEntry : entry.getValue()) {
        rsb.append(' ');
        rsb.append(valueEntry.value());
      }
      rsb.append('\n');
    }
    LOG.log(Level.INFO, rsb.toString());
  }

  /**
   * Pull up-to-date model parameters from server.
   * @param keys Column indices with which server stores the model parameters.
   */
  private NMFModel pullModels(final List<Integer> keys) {
    final Map<Integer, Vector> rMatrix = new HashMap<>(keys.size());
    final List<Vector> vectors = modelAccessor.pull(keys);
    for (int i = 0; i < keys.size(); ++i) {
      rMatrix.put(keys.get(i), vectors.get(i));
    }
    return new NMFModel(rMatrix);
  }

  /**
   * Processes one training data instance and update the intermediate model.
   * @param datum training data instance
   * @param model up-to-date NMFModel which is pulled from server
   * @param threadRGradient gradient matrix to update {@param model}

   */
  private void updateGradient(final NMFData datum, final NMFModel model,
                              final Map<Integer, Vector> threadRGradient) {
    final Vector lVec = datum.getVector(); // L_{i, *} : i-th row of L
    final Vector lGradSum;
    if (lambda != 0.0f) {
      // l2 regularization term. 2 * lambda * L_{i, *}
      lGradSum = lVec.scale(2.0f * lambda);
    } else {
      lGradSum = vectorFactory.createDenseZeros(rank);
    }

    for (final Pair<Integer, Float> column : datum.getColumns()) { // a pair of column index and value
      final int colIdx = column.getFirst();
      final Vector rVec = model.getRMatrix().get(colIdx); // R_{*, j} : j-th column of R
      final float error = lVec.dot(rVec) - column.getSecond(); // e = L_{i, *} * R_{*, j} - D_{i, j}

      // compute gradients
      // lGrad = 2 * e * R_{*, j}'
      // rGrad = 2 * e * L_{i, *}'
      final Vector lGrad;
      final Vector rGrad;

      lGrad = rVec.scale(2.0f * error);
      rGrad = lVec.scale(2.0f * error);

      // aggregate L matrix gradients
      lGradSum.addi(lGrad);

      // accumulate R matrix's gradient at threadRGradient
      accumulateRMatrixGradient(colIdx, rGrad, model, threadRGradient);
    }

    // update L matrix
    modelGenerator.getValidVector(lVec.axpy(-stepSize, lGradSum));
  }

  /**
   * Aggregate the model computed by multiple threads, to get the gradients to push.
   * gradient[j] = sum(gradient_t[j]) where j is the column index of the gradient matrix.
   * @param totalRGradients list of threadRGradients computed by trainer threads
   * @return aggregated gradient matrix
   */
  private Map<Integer, Vector> aggregateGradient(final List<Map<Integer, Vector>> totalRGradients) {
    final Map<Integer, Vector> aggregated = new HashMap<>();
    totalRGradients.forEach(threadRGradient -> threadRGradient.forEach((k, v) -> {
      if (aggregated.containsKey(k)) {
        aggregated.get(k).addi(v);
      } else {
        aggregated.put(k, v);
      }
    }));
    return aggregated;
  }

  /**
   * Push the gradients to parameter server.
   * @param gradients vectors indexed by column indices each of which is gradient of a column of R matrix.
   */
  private void pushAndResetGradients(final Map<Integer, Vector> gradients) {
    // push gradients
    for (final Map.Entry<Integer, Vector> entry : gradients.entrySet()) {
      modelAccessor.push(entry.getKey(), entry.getValue());
    }
    // clear gradients
    gradients.clear();
  }

  /**
   * Compute the loss value using the current models and given data instances.
   * May take long, so do not call frequently.
   * @param instances The training data instances to evaluate training loss.
   * @return the loss value, computed by the sum of the errors.
   */
  private float computeLoss(final Collection<NMFData> instances, final NMFModel model) {
    final Map<Integer, Vector> rMatrix = model.getRMatrix();

    float loss = 0.0f;
    for (final NMFData datum : instances) {
      final Vector lVec = datum.getVector(); // L_{i, *} : i-th row of L
      for (final Pair<Integer, Float> column : datum.getColumns()) { // a pair of column index and value
        final int colIdx = column.getFirst();
        final Vector rVec = rMatrix.get(colIdx); // R_{*, j} : j-th column of R
        final float error = lVec.dot(rVec) - column.getSecond(); // e = L_{i, *} * R_{*, j} - D_{i, j}
        loss += error * error;
      }
    }
    return loss;
  }

  /**
   * @param dataValues Dataset assigned to this worker
   * @return Keys to send pull requests, which are determined by existing columns in NMFData.
   */
  private List<Integer> getKeys(final Collection<NMFData> dataValues) {
    final ArrayList<Integer> keys = new ArrayList<>();
    final Set<Integer> keySet = Sets.newTreeSet();
    // aggregate column indices
    for (final NMFData datum : dataValues) {
      keySet.addAll(
          datum.getColumns()
              .stream()
              .distinct()
              .map(Pair::getFirst)
              .collect(Collectors.toList()));
    }
    keys.ensureCapacity(keySet.size());
    keys.addAll(keySet);
    return keys;
  }

  /**
   * Accumulates a new gradient into the R Matrix's gradient.
   * @param colIdx index of the column that the gradient is associated with
   * @param newGrad new gradient vector to accumulate
   * @param model up-to-date NMFModel which is pulled from server
   * @param threadRGradient gradient matrix to update {@param rMatrix}
   */
  private void accumulateRMatrixGradient(final int colIdx, final Vector newGrad, final NMFModel model,
                                         final Map<Integer, Vector> threadRGradient) {
    final Vector grad = threadRGradient.get(colIdx);
    if (grad == null) {
      // l2 regularization term. 2 * lambda * R_{*, j}
      if (lambda != 0.0D) {
        newGrad.axpy(2.0f * lambda, model.getRMatrix().get(colIdx));
      }
      threadRGradient.put(colIdx, newGrad);
    } else {
      grad.addi(newGrad);
    }
  }
  
  private EpochResult buildEpochResult(final float trainingLoss) {
    return EpochResult.newBuilder()
        .addAppMetric(MetricKeys.TRAINING_LOSS, trainingLoss)
        .build();
  }
}
