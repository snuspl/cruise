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
package edu.snu.cay.dolphin.examples.matmul;

import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public final class MatrixMulDataParser implements DataParser<List<IndexedVector>> {

  private final DataSet<LongWritable, Text> dataSet;

  private List<IndexedVector> result;
  private ParseException parseException;

  @Inject
  private MatrixMulDataParser(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public List<IndexedVector> get() throws ParseException {
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
    result = new ArrayList<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      final String text = keyValue.getSecond().toString().trim();
      if (text.startsWith("#") || text.length() == 0) {
        continue;
      }

      final String[] split = text.split("\\s+");
      final int numColumns = split.length - 1;
      final Vector row = new DenseVector(numColumns);

      try {
        for (int i = 0; i < numColumns; i++) {
          row.set(i, Double.valueOf(split[i + 1]));
        }
      } catch (final NumberFormatException e) {
        parseException = new ParseException("Parse failed: numbers should be DOUBLE");
        return;
      }

      result.add(new IndexedVector(Integer.valueOf(split[0]), row));
    }
  }
}
