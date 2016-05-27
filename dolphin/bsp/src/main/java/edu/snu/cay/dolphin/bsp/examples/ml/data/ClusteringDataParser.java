/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.bsp.examples.ml.data;

import edu.snu.cay.dolphin.bsp.core.DataParser;
import edu.snu.cay.dolphin.bsp.core.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public final class ClusteringDataParser implements DataParser<List<Vector>> {
  private static final Logger LOG = Logger.getLogger(ClusteringDataParser.class.getName());

  private final DataSet<LongWritable, Text> dataSet;
  private List<Vector> result;
  private ParseException parseException;

  @Inject
  public ClusteringDataParser(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public List<Vector> get() throws ParseException {
    if (result == null) {
      parse();
    }
    if (parseException != null) {
      throw parseException;
    }
    return result;
  }

  @Override
  public void parse() {
    final List<Vector> points = new ArrayList<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      final String text = keyValue.getSecond().toString().trim();
      if (text.startsWith("#") || text.length() == 0) {
        continue;
      }

      final String[] split = text.split("\\s+");
      final Vector point = new DenseVector(split.length);
      try {
        for (int i = 0; i < split.length; i++) {
          point.set(i, Double.valueOf(split[i]));
        }
        points.add(point);
      } catch (final NumberFormatException e) {
        parseException = new ParseException("Parse failed: each field should be a number");
        return;
      }
    }
    result = points;
  }
}
