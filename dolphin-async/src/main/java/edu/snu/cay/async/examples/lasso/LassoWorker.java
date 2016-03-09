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
package edu.snu.cay.async.examples.lasso;

import edu.snu.cay.async.Worker;
import edu.snu.cay.async.WorkerSynchronizer;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Worker} class for the LassoREEF application.
 * Based on lasso regression via stochastic coordinate descent, proposed in
 * S. Shalev-Shwartz and A. Tewari, Stochastic Methods for l1-regularized Loss Minimization, 2011.
 *
 * For each iteration, the worker pulls the whole model from the server,
 * and then randomly picks a dimension to update.
 * The worker computes and pushes the optimal model value for that particular dimension to
 * minimize the objective function - square loss with l1 regularization.
 *
 * All inner product values are cached in member fields.
 */
final class LassoWorker implements Worker {
  private static final Logger LOG = Logger.getLogger(LassoWorker.class.getName());

  /**
   * Factory object for creating new vectors.
   */
  private final VectorFactory vectorFactory;

  /**
   * Array of vectors containing the features of the input data.
   * Vector {@code j} represents the {@code j}-th feature of all input instances,
   * not the {@code j}-th instance.
   * Thus, the {@code i}-th instance can be formed by
   * concatenating the {@code i}-th elements of each vector.
   */
  private final Vector[] vecXArray;

  /**
   * Vector containing the target values of the input data.
   * The {@code i}-th element corresponds to the {@code i}-th input instance,
   * which can be found by concatenating the {@code i}-th element of each Vector in {@code vecXArray}.
   */
  private final Vector vecY;

  /**
   * Regularization constant.
   */
  private final double lambda;

  /**
   * The inner product values of each feature vector ({@code vecXArray}) and the target vector ({@code vecY}).
   * Specifically, {@code x2y[i] := vecXArray[i].dot(vecY)}.
   */
  private final double[] x2y;

  /**
   * The inner product values of the feature vectors ({@code vecXArray}).
   * Specifically, {@code x2x.get(i).get(j) := vecXArray[i].dot(vecXArray[j])}.
   * To reduce the size of this map, the smaller index (between {@code i} and {@code j})
   * should be used as the index for the outer map, and the bigger index for the inner map. <br>
   *
   * The reason for using a map and not a 2-D array is because there are cases where
   * we never use the inner product of {@code x2y[i]} and {@code x2y[j]}, and thus
   * allocating a double value for those cases would be a waste of space.
   */
  private final Map<Integer, Map<Integer, Double>> x2x;

  /**
   * Random number generator.
   */
  private final Random random;

  /**
   * Worker object for interacting with the parameter server.
   */
  private final ParameterWorker<Integer, Double, Double> worker;

  /**
   * Synchronization component for setting a global barrier across workers.
   */
  private final WorkerSynchronizer synchronizer;

  @Inject
  private LassoWorker(final LassoParser lassoParser,
                      final VectorFactory vectorFactory,
                      @Parameter(LassoREEF.Lambda.class) final double lambda,
                      final ParameterWorker<Integer, Double, Double> worker,
                      final WorkerSynchronizer synchronizer) {
    final Pair<Vector[], Vector> pair = lassoParser.parse();
    this.vectorFactory = vectorFactory;
    this.vecXArray = pair.getFirst();
    this.vecY = pair.getSecond();
    this.lambda = lambda;
    this.x2y = new double[vecXArray.length];
    this.x2x = new HashMap<>();
    this.random = new Random();
    this.worker = worker;
    this.synchronizer = synchronizer;
  }

  /**
   * {@inheritDoc}
   * Standardize input vectors w.r.t. each feature dimension, as well as the target vector,
   * to have 0 as the mean and the number of input instances as the variation.
   * Also pre-calculate inner products of input vectors and the target vector, for later use.
   */
  @Override
  public void initialize() {

    // TODO #396: We could skip feature scaling, since
    // it might negatively affect the algorithm for distributed environments.
    for (final Vector vecX : vecXArray) {
      standardize(vectorFactory, vecX);
    }
    standardize(vectorFactory, vecY);

    for (int index = 0; index < vecXArray.length; index++) {
      x2y[index] = vecXArray[index].dot(vecY);
    }

    synchronizer.globalBarrier();
  }

  /**
   * {@inheritDoc} <br>
   * 1) Pull model from server. <br>
   * 2) Pick dimension to update. <br>
   * 3) Compute the optimal value, (dot(x_i, y) - sigma_(i != j) (x_i, x_j) * model(j)) / N. <br>
   * - When computing the optimal value, only compute (x_i, x_j) * model(j) if model(j) != 0, for performance. <br>
   * - Reuse (x_i, x_j) when possible, from {@code x2x}. <br>
   * 4) Push value to server.
   */
  @Override
  public void run() {
    final double[] vecModel = new double[vecXArray.length];
    for (int index = 0; index < vecModel.length; index++) {
      vecModel[index] = worker.pull(index);
    }

    final int index = random.nextInt(vecModel.length);
    double dotValue = x2y[index];
    for (int modelIndex = 0; modelIndex < vecModel.length; modelIndex++) {
      if (vecModel[modelIndex] == 0 || index == modelIndex) {
        continue;
      }

      final int min = Math.min(index, modelIndex);
      final int max = Math.max(index, modelIndex);
      if (!x2x.containsKey(min)) {
        x2x.put(min, new HashMap<Integer, Double>());
      }
      if (!x2x.get(min).containsKey(max)) {
        x2x.get(min).put(max, vecXArray[index].dot(vecXArray[modelIndex]));
      }

      dotValue -= x2x.get(min).get(max) * vecModel[modelIndex];
    }

    dotValue /= vecY.length();
    worker.push(index, sthresh(dotValue, lambda));
  }

  /**
   * {@inheritDoc}
   * Pull and log the whole model.
   */
  @Override
  public void cleanup() {
    final double[] b = new double[vecXArray.length];
    for (int index = 0; index < b.length; index++) {
      b[index] = worker.pull(index);
      if (b[index] != 0) {
        LOG.log(Level.INFO, "Index {0}: value {1}", new Object[]{index, b[index]});
      }
    }
  }

  /**
   * Soft thresholding function, widely used with l1 regularization.
   */
  private static double sthresh(final double x, final double lambda) {
    if (Math.abs(x) <= lambda) {
      return 0;
    } else if (x >= 0) {
      return x - lambda;
    } else {
      return x + lambda;
    }
  }

  /**
   * Standardize vector {@code v} to have a mean of zero and a variation of {@code v.length()}.
   */
  private static void standardize(final VectorFactory vf, final Vector v) {
    double sum = 0;
    for (int i = 0; i < v.length(); i++) {
      sum += v.get(i);
    }
    final double mean = sum / v.length();
    v.subi(mean);

    double sqrsum = 0;
    for (int i = 0; i < v.length(); i++) {
      sqrsum += v.get(i) * v.get(i);
    }
    v.scalei(Math.sqrt(v.length() / sqrsum));
  }
}
