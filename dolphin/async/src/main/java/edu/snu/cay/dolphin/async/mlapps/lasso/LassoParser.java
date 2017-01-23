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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.dolphin.async.DataParser;
import edu.snu.cay.dolphin.async.mlapps.lasso.LassoParameters.NumFeatures;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

/**
 * Parser class for the LassoCDREEF application.
 * Input files should be in the following form:
 *
 * <p>
 *   x_11 x_12 x_13 x_14 ... x_1n y_1 <br>
 *   x_21 x_22 x_23 x_24 ... x_2n y_2 <br>
 *   ... <br>
 *   x_m1 x_m2 x_m3 x_m4 ... x_mn y_m <br>
 * </p>
 */
final class LassoParser implements DataParser<LassoData> {

  private final DataSet<LongWritable, Text> dataSet;
  private final VectorFactory vectorFactory;
  private final int numFeatures;

  @Inject
  private LassoParser(final DataSet<LongWritable, Text> dataSet,
                      final VectorFactory vectorFactory,
                      @Parameter(NumFeatures.class) final int numFeatures) {
    this.dataSet = dataSet;
    this.vectorFactory = vectorFactory;
    this.numFeatures = numFeatures;
  }

  @Override
  public List<LassoData> parse() {
    final List<LassoData> retList = new LinkedList<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      final String text = keyValue.getSecond().toString().trim();
      if (text.startsWith("#") || text.length() == 0) {
        // comments and empty lines
        continue;
      }

      final String[] split = text.split("\\s+");
      final double[] xValues = new double[numFeatures];
      for (int index = 0; index < numFeatures; index++) {
        xValues[index] = Double.parseDouble(split[index]);
      }
      final double yValue = Double.parseDouble(split[numFeatures]);
      final Vector feature = vectorFactory.createDense(xValues);
      retList.add(new LassoData(feature, yValue));
    }
    return retList;
  }
}
