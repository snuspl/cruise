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
package edu.snu.cay.async.examples.nmf;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.snu.cay.async.Worker;
import edu.snu.cay.async.WorkerSynchronizer;
import edu.snu.cay.async.examples.nmf.NMFParameters.*;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.utils.LongRangeUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
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
  private final double stepSize;
  private final double lambda;
  private final int batchSize;
  private final boolean printMatrices;
  private final int logPeriod;
  private final NMFModelGenerator modelGenerator;
  private final ArrayList<Integer> keys;
  private final Map<Integer, Vector> lMatrix;
  private final Map<Integer, Vector> rMatrix; // R matrix cache
  private final Map<Integer, Vector> gradients; // R matrix gradients

  private static final String DATA_TYPE = "NMF";
  private final DataIdFactory<Long> idFactory;
  private final MemoryStore<Long> memoryStore;

  // data key ranges assigned to this worker
  private Set<LongRange> dataKeyRanges;

  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  /**
   * Number of iterations.
   */
  private int iteration = 0;

  @Inject
  private NMFWorker(final NMFDataParser dataParser,
                    final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                    final WorkerSynchronizer workerSynchronizer,
                    @Parameter(StepSize.class) final double stepSize,
                    @Parameter(Lambda.class) final double lambda,
                    @Parameter(BatchSize.class) final int batchSize,
                    @Parameter(PrintMatrices.class) final boolean printMatrices,
                    @Parameter(LogPeriod.class) final int logPeriod,
                    final NMFModelGenerator modelGenerator,
                    final DataIdFactory<Long> idFactory,
                    final MemoryStore<Long> memoryStore) {
    this.parameterWorker = parameterWorker;
    this.workerSynchronizer = workerSynchronizer;
    this.dataParser = dataParser;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.batchSize = batchSize;
    this.printMatrices = printMatrices;
    this.logPeriod = logPeriod;
    this.modelGenerator = modelGenerator;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;

    this.keys = Lists.newArrayList();
    this.lMatrix = Maps.newHashMap();
    this.rMatrix = Maps.newHashMap();
    this.gradients = Maps.newHashMap();

    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  @Override
  public void initialize() {
    final List<NMFData> dataValues = dataParser.parse();
    final List<Long> dataKeys;

    try {
      dataKeys = idFactory.getIds(dataValues.size());
    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }

    memoryStore.putList(DATA_TYPE, dataKeys, dataValues);


    // TODO #302: initialize WorkloadPartition here

    // We should convert the ids into ranges, because the current MemoryStore API takes ranges not a list
    dataKeyRanges = LongRangeUtils.generateDenseLongRanges(new TreeSet<>(dataKeys));

    final Set<Integer> keySet = Sets.newTreeSet();
    // initialize L Matrix and aggregate column indices;
    for (final NMFData datum : dataValues) {
      final int rowIdx = datum.getRowIndex();
      lMatrix.put(rowIdx, modelGenerator.createRandomVector());

      for (final Pair<Integer, Double> column : datum.getColumns()) {
        keySet.add(column.getFirst());
      }
    }
    keys.ensureCapacity(keySet.size());
    keys.addAll(keySet);

    LOG.log(Level.INFO, "Batch Size = {0}", batchSize);
    LOG.log(Level.INFO, "Total number of keys = {0}", keys.size());
    LOG.log(Level.INFO, "Total number of input rows = {0}", dataValues.size());

    workerSynchronizer.globalBarrier();
  }

  private void saveRMatrixGradient(final int key, final Vector newGrad) {
    final Vector grad = gradients.get(key);
    if (grad == null) {
      gradients.put(key, newGrad);
    } else {
      grad.addi(newGrad);
    }
  }

  private void pushAndClearGradients() {
    // push gradients
    pushTracer.start();
    for (final Map.Entry<Integer, Vector> entry : gradients.entrySet()) {
      parameterWorker.push(entry.getKey(), entry.getValue());
    }
    pushTracer.end(gradients.size());
    // clear gradients
    gradients.clear();
  }

  private void pullRMatrix() {
    pullTracer.start();
    final List<Vector> vectors = parameterWorker.pull(keys);
    for (int i = 0; i < keys.size(); ++i) {
      rMatrix.put(keys.get(i), vectors.get(i));
    }
    pullTracer.end(keys.size());
  }

  private void resetTracers() {
    pushTracer.reset();
    pullTracer.reset();
    computeTracer.reset();
  }

  @Override
  public void run() {
    final long iterationBegin = System.currentTimeMillis();
    double loss = 0.0;
    int elemCount = 0;
    int rowCount = 0;
    resetTracers();

    // TODO #302: update dataKeyRanges when there's an update in assigned workload

    final List<NMFData> workload = new LinkedList<>();
    for (final LongRange range : dataKeyRanges) {
      final Map<Long, NMFData> subMap = memoryStore.getRange(DATA_TYPE, range.getMinimumLong(), range.getMaximumLong());
      workload.addAll(subMap.values());
    }

    pullRMatrix();

    for (final NMFData datum : workload) {
      computeTracer.start();
      final int rowIdx = datum.getRowIndex();
      final Vector lVec = lMatrix.get(rowIdx); // L_{i, *} : i-th row of L

      for (final Pair<Integer, Double> column : datum.getColumns()) { // a pair of column index and value
        final int colIdx = column.getFirst();
        final Vector rVec = rMatrix.get(colIdx); // R_{*, j} : j-th column of R
        final double error = lVec.dot(rVec) - column.getSecond(); // e = L_{i, *} * R_{*, j} - D_{i, j}

        // compute gradients with l2 regularization
        // lGrad = 2 * e * R_{*, j}' + 2 * lambda * L_{i, *}
        final Vector lGrad = rVec.scale(error / batchSize);
        if (lambda != 0.0D) {
          lGrad.axpy(lambda, lVec);
        }
        lGrad.scalei(2.0D * stepSize);
        // rGrad = 2 * e + L_{i, *}' + 2 * lambda + R_{j, *}
        final Vector rGrad = lVec.scale(error / batchSize);
        if (lambda != 0.0D) {
          rGrad.axpy(lambda, rVec);
        }
        rGrad.scalei(2.0D * stepSize);

        // update L matrix
        modelGenerator.getValidVector(lVec.subi(lGrad));

        // save R matrix gradients
        saveRMatrixGradient(colIdx, rGrad);

        // aggregate loss
        // iterative mean = c(t+1) = c(t) + (x - c(t)) / (t + 1)
        loss += (error * error - loss) / ++elemCount;
      }
      computeTracer.end(datum.getColumns().size());

      if (++rowCount % batchSize == 0) {
        pushAndClearGradients();
        pullRMatrix();
      }

      if (logPeriod > 0 && rowCount % logPeriod == 0) {
        final double elapsedTime = (System.currentTimeMillis() - iterationBegin) / 1000.0D;
        LOG.log(Level.INFO, "Iteration: {0}, Row Count: {1}, Loss: {2}, Avg Comp Per Row: {3}, " +
            "Sum Comp: {4}, Avg Pull: {5}, Sum Pull: {6}, Avg Push: {7}, " +
            "Sum Push: {8}, DvT: {9}, RvT: {10}, Elapsed Time: {11}",
            new Object[]{iteration, rowCount, String.format("%g", loss), computeTracer.avg(),
                computeTracer.sum(), pullTracer.avg(), pullTracer.sum(), pushTracer.avg(),
                pushTracer.sum(), elemCount / elapsedTime, rowCount / elapsedTime, elapsedTime});
      }
    }

    pushAndClearGradients();
    ++iteration;

    final double elapsedTime = (System.currentTimeMillis() - iterationBegin) / 1000.0D;
    LOG.log(Level.INFO, "End iteration: {0}, Row Count: {1}, Loss: {2}, Avg Comp Per Row: {3}, " +
            "Sum Comp: {4}, Avg Pull: {5}, Sum Pull: {6}, Avg Push: {7}, " +
            "Sum Push: {8}, DvT: {9}, RvT: {10}, Elapsed Time: {11}",
        new Object[]{iteration, rowCount, String.format("%g", loss), computeTracer.avg(),
            computeTracer.sum(), pullTracer.avg(), pullTracer.sum(), pushTracer.avg(),
            pushTracer.sum(), elemCount / elapsedTime, rowCount / elapsedTime, elapsedTime});
  }

  @Override
  public void cleanup() {
    // print generated matrices
    if (!printMatrices) {
      return;
    }
    // print L matrix
    final StringBuilder lsb = new StringBuilder();
    for (final Map.Entry<Integer, Vector> entry : lMatrix.entrySet()) {
      lsb.append(String.format("L(%d, *):", entry.getKey()));
      for (final VectorEntry valueEntry : entry.getValue()) {
        lsb.append(' ');
        lsb.append(valueEntry.value());
      }
      lsb.append('\n');
    }
    LOG.log(Level.INFO, lsb.toString());

    // print transposed R matrix
    pullRMatrix();
    final StringBuilder rsb = new StringBuilder();
    for (final Map.Entry<Integer, Vector> entry : rMatrix.entrySet()) {
      rsb.append(String.format("R(*, %d):", entry.getKey()));
      for (final VectorEntry valueEntry : entry.getValue()) {
        rsb.append(' ');
        rsb.append(valueEntry.value());
      }
      rsb.append('\n');
    }
    LOG.log(Level.INFO, rsb.toString());
  }
}
