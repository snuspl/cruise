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
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Data parser for neural network.
 *
 * Parses Numpy compatible plain text file.
 */
public final class NeuralNetworkDataParser {
  private static final Logger LOG = Logger.getLogger(NeuralNetworkDataParser.class.getName());

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
        try {
          final List<DataInstance> dataInstances = readNumpy(new ByteArrayInputStream(text.getBytes()));
          for (final DataInstance dataInstance : dataInstances) {
            if (dataInstance.isValidation) {
              validationBatchGenerator.push(dataInstance.data, dataInstance.label);
            } else {
              trainingBatchGenerator.push(dataInstance.data, dataInstance.label);
            }
          }
        } catch (final IOException e) {
          throw new RuntimeException("Failed to parse data: ", e);
        }
      }

      trainingBatchGenerator.cleanUp();
      validationBatchGenerator.cleanUp();

      result = dataList;
    }

    return result;
  }
  /**
   * Loads data instances from an input stream of a Numpy-compatible plain text file with the specified delimiter.
   * @param inputStream a Numpy-compatible plain text input stream
   * @return a loaded data instances
   * @throws IOException
   */
  private List<DataInstance> readNumpy(final InputStream inputStream) throws IOException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    final List<DataInstance> dataList = new ArrayList<>();
    int dataLength = -1;
    while ((line = reader.readLine()) != null) {
      final String[] data = line.trim().split(delimiter);
      if (dataLength < 0) {
        dataLength = data.length;
      } else {
        if (data.length != dataLength) {
          throw new RuntimeException("Data has inconsistent length");
        }
      }
      final float[] floatData = new float[dataLength - 2];
      for (int i = 0; i < dataLength - 2; i++) {
        floatData[i] = Float.parseFloat(data[i]);
      }
      final int label = Integer.parseInt(data[dataLength - 2]);
      final boolean isValidation = Integer.parseInt(data[dataLength - 1]) == 1;
      dataList.add(new DataInstance(floatData, label, isValidation));
    }

    return dataList;
  }

  /**
   * Class for a data instance including label and validation bit.
   */
  private class DataInstance {
    private final float[] data;
    private final int label;
    private final boolean isValidation;

    public DataInstance(final float[] data,
                        final int label,
                        final boolean isValidation) {
      this.data = data;
      this.label = label;
      this.isValidation = isValidation;
    }
  };

  /**
   * Class for generating batch matrix and an array of labels with the specified batch size.
   */
  private class BatchGenerator {
    private final List<NeuralNetworkData> dataList;
    private final boolean isValidation;
    private final List<float[]> dataInstanceList;
    private final List<Integer> labelList;

    public BatchGenerator(final List<NeuralNetworkData> dataList,
                          final boolean isValidation) {
      this.dataList = dataList;
      this.isValidation = isValidation;
      this.dataInstanceList = new ArrayList<>(batchSize);
      this.labelList = new ArrayList<>(batchSize);
    }

    /**
     * @return the number of aggregated data.
     */
    public int size() {
      return dataInstanceList.size();
    }

    /**
     * Pushes a matrix and label. When the specified batch size of matrix and label data have been gathered,
     * a new batch data is pushed to the list of data.
     *
     * @param data  a single datum
     * @param label a label for the datum.
     */
    public void push(final float[] data, final int label) {
      dataInstanceList.add(data);
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
      final NeuralNetworkData data = new NeuralNetworkData(makeBatch(dataInstanceList),
          ArrayUtils.toPrimitive(labelList.toArray(new Integer[labelList.size()])),
          isValidation);

      dataList.add(data);
      dataInstanceList.clear();
      labelList.clear();
    }

    /**
     * Generates a batch input matrix with the specified list of input data.
     *
     * @param inputs a list of input data
     * @return a batch input matrix
     */
    private Matrix makeBatch(final List<float[]> inputs) {
      if (inputs.size() == 0) {
        throw new IllegalArgumentException("At least one input is needed to make batch");
      }

      final Matrix ret = matrixFactory.create(inputs.get(0).length, inputs.size());
      for (int i = 0; i < inputs.size(); i++) {
        ret.putColumn(i, matrixFactory.create(inputs.get(i)));
      }
      return ret;
    }
  }
}
