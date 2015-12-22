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
package edu.snu.cay.dolphin.examples.ml.data;

import edu.snu.cay.dolphin.breeze.Vector;
import edu.snu.cay.dolphin.breeze.VectorFactory;
import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.examples.ml.parameters.Dimension;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ClassificationDenseDataParser implements DataParser<List<Row>> {
  private static final Logger LOG = Logger.getLogger(ClassificationDenseDataParser.class.getName());

  private final AtomicInteger count = new AtomicInteger(0);
  private final int positiveLabel = 1;
  private final int negativeLabel = -1;
  private final int dimension;
  private final DataSet<LongWritable, Text> dataSet;
  private final VectorFactory vectorFactory;
  private List<Row> result;
  private ParseException parseException;

  @Inject
  public ClassificationDenseDataParser(@Parameter(Dimension.class) final int dimension,
                                       final DataSet<LongWritable, Text> dataSet,
                                       final VectorFactory vectorFactory) {
    this.dimension = dimension;
    this.dataSet = dataSet;
    this.vectorFactory = vectorFactory;
  }

  @Override
  public List<Row> get() throws ParseException {
    LOG.log(Level.INFO, "ClassificationDenseDataParser called {0} times", count.incrementAndGet());
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
    LOG.log(Level.INFO, "Trying to parse!");
    result = new ArrayList<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {

      final String text = keyValue.getSecond().toString().trim();
      if (text.startsWith("#") || text.length() == 0) {
        continue;
      }

      final String[] split = text.split("\\s+");
      if (split.length != dimension + 1) {
        parseException = new ParseException(
            String.format("Parse failed: the number of features should be %d", dimension));
        return;
      }

      final int output;
      final Vector feature = vectorFactory.newDenseVector(split.length);
      try {
        output = Integer.valueOf(split[dimension]);
        if (output != positiveLabel && output != negativeLabel) {
          throw new NumberFormatException();
        }
      } catch (final NumberFormatException e) {
        parseException = new ParseException(
            String.format("Parse failed: last column value should be either %d or %d", positiveLabel, negativeLabel));
        return;
      }
      try {
        for (int i = 0; i < dimension; i++) {
          feature.set(i, Double.valueOf(split[i]));
        }
        feature.set(dimension, 1);
      } catch (final NumberFormatException e) {
        parseException = new ParseException("Parse failed: numbers should be DOUBLE");
        return;
      }
      result.add(new Row(output, feature));
    }
  }
}
