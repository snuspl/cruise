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
package edu.snu.cay.async.examples.recommendation;

import edu.snu.cay.async.examples.recommendation.NMFParameters.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser that parses data used for non-negative matrix factorization.
 *
 * Input files should be stored in the following format.
 * <p>
 *   [row index] [column index] [value] <br>
 *   [row index] [column index] [value] <br>
 *   ... <br>
 *   [row index] [column index] [value] <br>
 * </p>
 * Indices used in this format is zero-based.
 */
final class NMFDataParser {

  private final DataSet<LongWritable, Text> dataSet;
  private final int rows;
  private final int cols;

  @Inject
  private NMFDataParser(final DataSet<LongWritable, Text> dataSet,
                        @Parameter(NumRows.class) final int rows,
                        @Parameter(NumColumns.class) final int cols) {
    this.dataSet = dataSet;
    this.rows = rows;
    this.cols = cols;
  }

  public List<NMFData> parse() {
    final List<NMFData> result = new ArrayList<>();

    for (final Pair<LongWritable, Text> ketValue : dataSet) {
      final String line = ketValue.getSecond().toString().trim();
      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }

      final String[] split = line.split("\\s+");
      if (split.length != 3) {
        throw new RuntimeException("Failed to parse: the number of elements in each line should be 3");
      }

      final int rowIndex;
      final int colIndex;
      final double value;

      try {
        rowIndex = Integer.valueOf(split[0]);
        colIndex = Integer.valueOf(split[1]);
      } catch (final NumberFormatException e) {
        throw new RuntimeException("Failed to parse: numbers for indices should be integer.");
      }

      if (rowIndex >= rows) {
        throw new RuntimeException(
            String.format("Invalid row index %d. It should be less than # of input rows %d", rowIndex, rows));
      }
      if (colIndex >= cols) {
        throw new RuntimeException(
            String.format("Invalid column index %d. It should be less than # of input columns %d", colIndex, cols));
      }

      try {
        value = Double.valueOf(split[2]);
      } catch (final NumberFormatException e) {
        throw new RuntimeException("Failed to parse: numbers for values should be double.");
      }

      if (value < 0) {
        throw new RuntimeException("Failed to parse: numbers should be greater than or equal to zero.");
      }

      result.add(new NMFData(rowIndex, colIndex, value));
    }
    return result;
  }
}
