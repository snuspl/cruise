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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


/**
 * A data parser for sparse vector regression input.
 * Assumes the following data format.
 * <p>
 *   [yValue] [index]:[value] [index]:[value] ...
 * </p>
 */
final class LassoETParser implements DataParser<LassoData> {

  private final VectorFactory vectorFactory;
  private final int numFeatures;

  @Inject
  private LassoETParser(final VectorFactory vectorFactory,
                        @Parameter(NumFeatures.class) final int numFeatures) {
    this.vectorFactory = vectorFactory;
    this.numFeatures = numFeatures;
  }

  @Override
  public List<LassoData> parse(final Collection<String> rawData) {
    final List<LassoData> retList = new LinkedList<>();

    for (final String value : rawData) {
      final String line = value.trim();
      if (line.startsWith("#") || line.length() == 0) {
        // comments and empty lines
        continue;
      }

      final String[] split = line.split("\\s+|:");
      final float yValue = Float.parseFloat(split[0]);

      final int[] indices = new int[split.length / 2];
      final float[] data = new float[split.length / 2];
      for (int index = 0; index < split.length / 2; ++index) {
        indices[index] = Integer.parseInt(split[2 * index + 1]);
        data[index] = Float.parseFloat(split[2 * index + 2]);
      }

      final Vector feature = vectorFactory.createSparse(indices, data, numFeatures);
      retList.add(new LassoData(feature, yValue));
    }

    return retList;
  }
}
