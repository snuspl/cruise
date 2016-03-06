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

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

/**
 * Parser class for the LassoREEF application.
 * Input files should be in the following form:
 *
 * <p>
 *   x_11 x_12 x_13 x_14 ... x_1n y_1 <br>
 *   x_21 x_22 x_23 x_24 ... x_2n y_2 <br>
 *   ... <br>
 *   x_m1 x_m2 x_m3 x_m4 ... x_mn y_m <br>
 * </p>
 */
final class LassoParser {

  private final DataSet<LongWritable, Text> dataSet;
  private final VectorFactory vectorFactory;
  private final int numFeatures;

  @Inject
  private LassoParser(final DataSet<LongWritable, Text> dataSet,
                      final VectorFactory vectorFactory,
                      @Parameter(LassoREEF.NumFeatures.class) final int numFeatures) {
    this.dataSet = dataSet;
    this.vectorFactory = vectorFactory;
    this.numFeatures = numFeatures;
  }

  Pair<Vector[], Vector> parse() {
    final List<double[]> dataX = new LinkedList<>();
    final List<Double> dataY = new LinkedList<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      final String line = keyValue.getSecond().toString().trim();
      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }

      final String[] split = line.split("\\s+");
      final double[] data = new double[numFeatures];
      for (int index = 0; index < numFeatures; index++) {
        data[index] = Double.parseDouble(split[index]);
      }
      dataX.add(data);
      dataY.add(Double.parseDouble(split[numFeatures]));
    }

    // Input vectors must be formed for each dimension, not for each instance.
    // Thus we transpose the 2d array before creating vectors.
    final double[][] arrayX = new double[dataX.size()][];
    final double[][] transposedArrayX = transpose(dataX.toArray(arrayX));
    final Vector[] vecX = new Vector[transposedArrayX.length];
    for (int arrayIndex = 0; arrayIndex < transposedArrayX.length; arrayIndex++) {
      vecX[arrayIndex] = vectorFactory.newDenseVector(transposedArrayX[arrayIndex]);
    }

    final double[] arrayY = new double[dataY.size()];
    int arrayIndex = 0;
    for (final double y : dataY) {
      arrayY[arrayIndex++] = y;
    }
    final Vector vecY = vectorFactory.newDenseVector(arrayY);

    return new Pair<>(vecX, vecY);
  }

  private double[][] transpose(final double[][] array) {
    final double[][] retArray = new double[array[0].length][array.length];
    for (int i = 0; i < array.length; i++) {
      for (int j = 0; j < array[0].length; j++) {
        retArray[j][i] = array[i][j];
      }
    }

    return retArray;
  }
}
