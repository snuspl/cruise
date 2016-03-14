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
package edu.snu.cay.async.examples.recommendation;

import edu.snu.cay.async.Worker;
import edu.snu.cay.async.WorkerSynchronizer;
import edu.snu.cay.async.examples.recommendation.NMFParameters.*;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker for non-negative matrix factorization via SGD.
 *
 * Assumes that indices in {@link NMFData} are one-based.
 */
final class NMFWorker implements Worker {

  private static final Logger LOG = Logger.getLogger(NMFWorker.class.getName());

  private final ParameterWorker<Integer, Vector, Vector> parameterWorker;
  private final WorkerSynchronizer workerSynchronizer;
  private final NMFDataParser dataParser;
  private final List<NMFData> workload = new ArrayList<>();
  private final int numRows;
  private final int numColumns;
  private final double stepSize;
  private final double lambda;

  private int iteration = 0;

  @Inject
  private NMFWorker(final NMFDataParser dataParser,
                    final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                    final WorkerSynchronizer workerSynchronizer,
                    @Parameter(NumRows.class) final int numRows,
                    @Parameter(NumColumns.class) final int numColumns,
                    @Parameter(StepSize.class) final double stepSize,
                    @Parameter(Lambda.class) final double lambda) {
    this.parameterWorker = parameterWorker;
    this.workerSynchronizer = workerSynchronizer;
    this.dataParser = dataParser;
    this.numRows = numRows;
    this.numColumns = numColumns;
    this.stepSize = stepSize;
    this.lambda = lambda;
  }

  @Override
  public void initialize() {
    workload.addAll(dataParser.parse());
    workerSynchronizer.globalBarrier();
  }

  @Override
  public void run() {
    double loss = 0.0;

    for (final NMFData datum : workload) {
      final int lIndex = -datum.getRowIndex(); // use an negative index for L matrix.
      final int rIndex = datum.getColIndex();

      final Vector lVec = parameterWorker.pull(lIndex); // L_{i, *} : i-th row of L
      final Vector rVec = parameterWorker.pull(rIndex); // R_{*, j} : j-th column of R
      final double error = lVec.dot(rVec) - datum.getValue(); // e = L_{i, *} * R_{*, j} - D_{i, j}

      // compute gradients with l2 regularization
      final Vector lGrad = rVec.scale(error).axpy(lambda, lVec); // e * R_{*, j}' + lambda * L_{i, *}
      lGrad.scalei(2.0 * stepSize);
      parameterWorker.push(lIndex, lGrad);

      final Vector rGrad = lVec.scale(error).axpy(lambda, rVec); // e * L_{i, *}' + lambda * R_{*, j}
      rGrad.scalei(2.0 * stepSize);
      parameterWorker.push(rIndex, rGrad);

      // aggregate loss
      loss += error * error;
    }

    loss /= workload.size();

    LOG.log(Level.INFO, "Iteration: {0}, Loss: {1}", new Object[]{iteration, loss});

    ++iteration;
  }

  @Override
  public void cleanup() {
    // print generated matrices
    // print L
    System.out.println("L=");
    for (int i = 1; i <= numRows; ++i) {
      for (final VectorEntry valueEntry : parameterWorker.pull(-i)) {
        System.out.print(" " + valueEntry.value());
      }
      System.out.println();
    }
    // print transposed R (R')
    System.out.println("R'=");
    for (int i = 1; i <= numColumns; ++i) {
      for (final VectorEntry valueEntry : parameterWorker.pull(i)) {
        System.out.print(" " + valueEntry.value());
      }
      System.out.println();
    }
    workload.clear();
  }
}
