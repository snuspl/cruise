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
import edu.snu.cay.dolphin.async.DataParser;
import edu.snu.cay.dolphin.async.mlapps.gbt.GBTParameters.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;

/**
 * A data parser for sparse vector classification input.
 * Assumes the following data format.
 * <p>
 *   [label] [index]:[value] [index]:[value] ...
 * </p>
 */
final class GBTDataParser implements DataParser<GBTData> {

  private final DataSet<LongWritable, Text> dataSet;
  private final VectorFactory vectorFactory;
  private final int numFeatures;

  @Inject
  private GBTDataParser(final DataSet<LongWritable, Text> dataSet,
                        final VectorFactory vectorFactory,
                        @Parameter(NumFeatures.class) final int numFeatures) {
    this.dataSet = dataSet;
    this.vectorFactory = vectorFactory;
    this.numFeatures = numFeatures;
  }

  @Override
  public List<GBTData> parse() {
    final List<GBTData> retList = new LinkedList<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      final String text = keyValue.getSecond().toString().trim();
      if (text.startsWith("#") || text.length() == 0) {
        // comments and empty lines
        continue;
      }
  
      final String[] split = text.split("\\s+|:");
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
