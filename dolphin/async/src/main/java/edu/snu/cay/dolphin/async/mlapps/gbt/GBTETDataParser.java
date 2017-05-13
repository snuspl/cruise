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
package edu.snu.cay.dolphin.async.mlapps.gbt;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.dolphin.async.mlapps.gbt.GBTParameters.NumFeatures;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Parser class for the GBT application.
 * Input files should be in the following form:
 *
 * <p>
 *   x_11 x_12 x_13 x_14 ... x_1n y_1 <br>
 *   x_21 x_22 x_23 x_24 ... x_2n y_2 <br>
 *   ... <br>
 *   x_m1 x_m2 x_m3 x_m4 ... x_mn y_m <br>
 * </p>
 */
final class GBTETDataParser implements DataParser<GBTData> {

  private final VectorFactory vectorFactory;
  private final int numFeatures;

  @Inject
  private GBTETDataParser(final VectorFactory vectorFactory,
                          @Parameter(NumFeatures.class) final int numFeatures) {
    this.vectorFactory = vectorFactory;
    this.numFeatures = numFeatures;
  }

  @Override
  public List<GBTData> parse(final Collection<String> rawData) {
    final List<GBTData> retList = new LinkedList<>();
    for (final String datum : rawData) {
      final String line = datum.trim();
      if (line.startsWith("#") || line.length() == 0) {
        // comments and empty lines
        continue;
      }

      final String[] split = line.split("\\s+|:");
      final double value = Integer.parseInt(split[0]);

      final int[] indices = new int[split.length / 2];
      final double[] data = new double[split.length / 2];
      for (int index = 0; index < split.length / 2; ++index) {
        indices[index] = Integer.parseInt(split[2 * index + 1]);
        data[index] = Double.parseDouble(split[2 * index + 2]);
      }
      final Vector feature = vectorFactory.createSparse(indices, data, numFeatures);
      retList.add(new GBTData(feature, value));
    }

    return retList;
  }
}
