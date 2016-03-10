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
import java.util.LinkedList;
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
 * In this format, one-based indexing is used.
 */
final class NMFDataParser {

  private final DataSet<LongWritable, Text> dataSet;
  private final int numRows;
  private final int numCols;

  @Inject
  private NMFDataParser(final DataSet<LongWritable, Text> dataSet,
                        @Parameter(NumRows.class) final int numRows,
                        @Parameter(NumColumns.class) final int numCols) {
    this.dataSet = dataSet;
    this.numRows = numRows;
    this.numCols = numCols;
  }

  public List<NMFData> parse() {
    final List<NMFData> result = new LinkedList<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      final String line = keyValue.getSecond().toString().trim();
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
        throw new RuntimeException("Failed to parse: numbers for indices should be integer.", e);
      }

      if (rowIndex > numRows) {
        throw new RuntimeException(
            String.format("Invalid row index %d. It should be less than # of input rows %d", rowIndex, numRows));
      }

      if (colIndex > numCols) {
        throw new RuntimeException(
            String.format("Invalid column index %d. It should be less than # of input columns %d", colIndex, numCols));
      }

      if (rowIndex <= 0 || colIndex <= 0) {
        throw new RuntimeException("Invalid indices. It should be greater than zero");
      }

      try {
        value = Double.valueOf(split[2]);
      } catch (final NumberFormatException e) {
        throw new RuntimeException("Failed to parse: numbers for values should be double.", e);
      }

      if (value < 0) {
        throw new RuntimeException("Failed to parse: numbers should be greater than or equal to zero.");
      }

      result.add(new NMFData(rowIndex, colIndex, value));
    }
    return result;
  }
}
