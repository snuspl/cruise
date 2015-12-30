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

import edu.snu.cay.common.math.vector.Vector;
import edu.snu.cay.common.math.vector.VectorFactory;
import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.examples.ml.parameters.Dimension;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A data parser for sparse vector classification input.
 * Assumes the following data format.
 * <p>
 *   [output] [index]:[value] [index]:[value] ...
 * </p>
 */
public final class ClassificationSparseDataParser implements DataParser<List<Row>> {
  private static final Logger LOG = Logger.getLogger(ClassificationSparseDataParser.class.getName());

  private final AtomicInteger count = new AtomicInteger(0);
  private final int positiveLabel = 1;
  private final int negativeLabel = -1;
  private final int dimension;
  private final DataSet<LongWritable, Text> dataSet;
  private final VectorFactory vectorFactory;
  private List<Row> result;
  private ParseException parseException;

  @Inject
  public ClassificationSparseDataParser(@Parameter(Dimension.class) final int dimension,
                                        final DataSet<LongWritable, Text> dataSet,
                                        final VectorFactory vectorFactory) {
    this.dimension = dimension;
    this.dataSet = dataSet;
    this.vectorFactory = vectorFactory;
  }

  @Override
  public List<Row> get() throws ParseException {
    LOG.log(Level.INFO, "ClassificationSparseDataParser called {0} times", count.incrementAndGet());
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
      final int output;
      final Vector feature;

      // parse output
      try {
        output = Integer.valueOf(split[0]);

        if (output != positiveLabel && output != negativeLabel) {
          throw new NumberFormatException();
        }
      } catch (final NumberFormatException e) {
        parseException = new ParseException(
            String.format("Parse failed: last column value should be either %d or %d", positiveLabel, negativeLabel));
        return;
      }

      // parse a feature vector
      try {
        feature = parseFeatureVector(Arrays.copyOfRange(split, 1, split.length));
      } catch (final ParseException pe) {
        parseException = pe;
        return;
      }

      result.add(new Row(output, feature));
    }
  }

  /**
   * Parses the string argument as a pair of an index and a value.
   * @param elemString a string that uses the format "index:value".
   * @return a pair of an index and a value.
   * @throws ParseException if the specified string is invalid.
   */
  private Pair<Integer, Double> parseElement(final String elemString) throws ParseException {
    try {
      final String[] split = elemString.split(":");
      if (split.length != 2) {
        throw new ParseException("Parse failed: the format of each element of a sparse vector must be [index]:[value]");
      }
      return new Pair<>(Integer.valueOf(split[0]), Double.valueOf(split[1]));
    } catch (final NumberFormatException e) {
      throw new ParseException("Parse failed: invalid number format " + e);
    }
  }

  /**
   * Parses the specified array of strings as a sparse feature vector.
   * @param split an array of strings that represent elements of feature vector.
   * @return a feature vector.
   * @throws ParseException if a string element of the specified array is invalid.
   */
  private Vector parseFeatureVector(final String[] split) throws ParseException {
    final Vector ret = vectorFactory.newSparseVector(dimension + 1); // +1 for a constant term
    for (final String elementString : split) {
      final Pair<Integer, Double> elementPair = parseElement(elementString);
      ret.set(elementPair.getFirst(), elementPair.getSecond());
    }
    ret.set(dimension, 1); // a constant term
    return ret;
  }
}
