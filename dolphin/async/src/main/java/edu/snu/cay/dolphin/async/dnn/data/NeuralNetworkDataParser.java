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
package edu.snu.cay.dolphin.async.dnn.data;

import edu.snu.cay.dolphin.async.dnn.NeuralNetworkParameters.Delimiter;
import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters.BatchSize;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Data parser for neural network.
 *
 * Parses Numpy compatible plain text file.
 */
public final class NeuralNetworkDataParser {

  private final MatrixFactory matrixFactory;
  private final DataSet<LongWritable, Text> dataSet;
  private final String delimiter;
  private final int batchSize;
  private List<NeuralNetworkData> result;

  /**
   * @param injector the injector having the matrix factory configuration
   * @param dataSet the set of unparsed input data
   * @param delimiter the delimiter used in the input file
   * @param batchSize the size of input batch
   */
  @Inject
  private NeuralNetworkDataParser(final Injector injector,
                                  final DataSet<LongWritable, Text> dataSet,
                                  @Parameter(Delimiter.class) final String delimiter,
                                  @Parameter(BatchSize.class) final int batchSize) {
    try {
      this.matrixFactory = injector.forkInjector().getInstance(MatrixFactory.class);
    } catch (final InjectionException ie) {
      throw new RuntimeException("InjectException occurred while injecting a matrix factory", ie);
    }
    this.dataSet = dataSet;
    this.delimiter = delimiter;
    this.batchSize = batchSize;
  }

  public List<NeuralNetworkData> get() {
    if (result == null) {
      final List<NeuralNetworkData> dataList = new ArrayList<>();
      final BatchGenerator trainingBatchGenerator = new BatchGenerator(dataList, false);
      final BatchGenerator validationBatchGenerator = new BatchGenerator(dataList, true);

      for (final Pair<LongWritable, Text> keyValue : dataSet) {
        final String text = keyValue.getSecond().toString().trim();
        if (text.startsWith("#") || 0 == text.length()) {
          continue;
        }

        final String[] stringData = text.split(delimiter);
        final int dataLength = stringData.length;
        final float[] floatData = new float[dataLength - 2];
        for (int i = 0; i < dataLength - 2; i++) {
          floatData[i] = Float.parseFloat(stringData[i]);
        }
        final int label = Integer.parseInt(stringData[dataLength - 2]);
        final boolean isValidation = Integer.parseInt(stringData[dataLength - 1]) == 1;
        if (isValidation) {
          validationBatchGenerator.push(floatData, label);
        } else {
          trainingBatchGenerator.push(floatData, label);
        }
      }

      trainingBatchGenerator.cleanUp();
      validationBatchGenerator.cleanUp();

      result = dataList;
    }

    return result;
  }

  /**
   * Class for generating batch matrix and an array of labels with the specified batch size.
   */
  private class BatchGenerator {
    private final List<NeuralNetworkData> dataList;
    private final boolean isValidation;
    private final List<float[]> dataArray;
    private final List<Integer> labelList;

    BatchGenerator(final List<NeuralNetworkData> dataList,
                          final boolean isValidation) {
      this.dataList = dataList;
      this.isValidation = isValidation;
      this.dataArray = new ArrayList<>(batchSize);
      this.labelList = new ArrayList<>(batchSize);
    }

    /**
     * @return the number of aggregated data.
     */
    public int size() {
      return dataArray.size();
    }

    /**
     * Pushes a matrix and label. When the specified batch size of matrix and label data have been gathered,
     * a new batch data is pushed to the list of data.
     *
     * @param data  a single datum
     * @param label a label for the datum.
     */
    public void push(final float[] data, final int label) {
      dataArray.add(data);
      labelList.add(label);
      if (size() == batchSize) {
        makeAndAddBatch();
      }
    }

    /**
     * Makes a batch with the matrix and label data that have been pushed and adds it to the list of data,
     * if the matrix and label data that has not been added exist.
     */
    public void cleanUp() {
      if (size() > 0) {
        makeAndAddBatch();
      }
    }

    /**
     * Makes a batch with the matrix and label data that have been pushed and adds it to the list of data.
     */
    private void makeAndAddBatch() {
      final NeuralNetworkData data = new NeuralNetworkData(makeBatch(dataArray),
          ArrayUtils.toPrimitive(labelList.toArray(new Integer[labelList.size()])),
          isValidation);

      dataList.add(data);
      dataArray.clear();
      labelList.clear();
    }

    /**
     * Generates a batch input {@link Matrix} with the specified list of input data.
     *
     * @param inputs a list of input data
     * @return a batch input {@link Matrix}
     */
    private Matrix makeBatch(final List<float[]> inputs) {
      if (inputs.size() == 0) {
        throw new IllegalArgumentException("At least one input is needed to make batch");
      }
      final int dataSize = inputs.get(0).length; // all input arrays should have the same length
      final float[] batch = new float[inputs.size() * dataSize];
      for (int i = 0; i < inputs.size(); i++) {
        System.arraycopy(inputs.get(i), 0, batch, dataSize * i, dataSize);
      }
      final Matrix ret = matrixFactory.create(batch, dataSize, inputs.size());
      return ret;
    }
  }
}
