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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static edu.snu.cay.dolphin.async.mlapps.mlr.MLRParameters.NumClasses;

/**
 * A data parser for sparse vector classification input.
 * Assumes the following data format.
 * <p>
 *   [label] [index]:[value] [index]:[value] ...
 * </p>
 */
final class MLRETDataParser implements DataParser<MLRData> {

  private final VectorFactory vectorFactory;
  private final int numClasses;
  private final int numFeatures;

  @Inject
  private MLRETDataParser(final VectorFactory vectorFactory,
                          @Parameter(NumClasses.class)final int numClasses,
                          @Parameter(NumFeatures.class) final int numFeatures) {
    this.vectorFactory = vectorFactory;
    this.numClasses = numClasses;
    this.numFeatures = numFeatures;
  }

  @Override
  public List<MLRData> parse(final Collection<String> rawData) {
    final List<MLRData> result = new LinkedList<>();

    for (final String value : rawData) {
      final String line = value.trim();
      if (line.startsWith("#") || line.length() == 0) {
        // comments and empty lines
        continue;
      }

      final String[] split = line.split("\\s+|:");
      final int label = Integer.parseInt(split[0]);
      if (label < 0 || label > numClasses) {
        throw new RuntimeException(String.format("Label should be >= %d and < %d%n%s", 0, numClasses, line));
      }

      final int[] indices = new int[split.length / 2];
      final float[] data = new float[split.length / 2];
      for (int index = 0; index < split.length / 2; ++index) {
        indices[index] = Integer.parseInt(split[2 * index + 1]);
        data[index] = Float.parseFloat(split[2 * index + 2]);
      }

      final Vector feature = vectorFactory.createSparse(indices, data, numFeatures);
      result.add(new MLRData(feature, label));
    }
    return result;
  }
}
