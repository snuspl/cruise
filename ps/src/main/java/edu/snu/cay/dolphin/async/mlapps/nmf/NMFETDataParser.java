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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import edu.snu.cay.services.et.evaluator.api.DataParser;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Parser that parses data used for non-negative matrix factorization.
 *
 * Input files should be stored in the following format.
 * <p>
 *   [row index]: [column index],[value] [column index],[value] ... [column index],[value]<br>
 *   [row index]: [column index],[value] [column index],[value] ... [column index],[value]<br>
 *   ... <br>
 *   [row index]: [column index],[value] [column index],[value] ... [column index],[value]<br>
 * </p>
 * In this format, one-based indexing is used.
 */
final class NMFETDataParser implements DataParser<NMFData> {
  private final NMFModelGenerator modelGenerator;

  @Inject
  private NMFETDataParser(final NMFModelGenerator modelGenerator) {
    this.modelGenerator = modelGenerator;
  }

  private List<Pair<Integer, Float>> parseColumns(final String columnsString) {
    final String[] columns = columnsString.split("\\s+");
    final List<Pair<Integer, Float>> result = new ArrayList<>(columns.length);

    for (final String column : columns) {
      final int index;
      final float value;

      final String[] split = column.split(",");
      if (split.length != 2) {
        throw new RuntimeException("Failed to parse: each column string should follow the format [index],[value]");
      }
      try {
        index = Integer.valueOf(split[0]);
      } catch (final NumberFormatException e) {
        throw new RuntimeException("Failed to parse: numbers for indices should be integer", e);
      }
      try {
        value = Float.valueOf(split[1]);
      } catch (final NumberFormatException e) {
        throw new RuntimeException("Failed to parse: numbers for values should be Float.", e);
      }

      // check validity of values
      if (index <= 0) {
        throw new RuntimeException("Failed to parse: invalid indices. It should be greater than zero");
      }
      if (value < 0) {
        throw new RuntimeException("Failed to parse: numbers should be greater than or equal to zero.");
      }
      result.add(new Pair<>(index, value));
    }
    return result;
  }

  @Override
  public List<NMFData> parse(final Collection<String> rawData) {
    final List<NMFData> result = new LinkedList<>();

    for (final String value : rawData) {
      final String line = value.trim();
      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }

      final String[] split = line.split(":", 2);
      final int rowIndex;

      try {
        rowIndex = Integer.valueOf(split[0]);
      } catch (final NumberFormatException e) {
        throw new RuntimeException("Failed to parse: numbers for indices should be integer", e);
      }
      if (rowIndex <= 0) {
        throw new RuntimeException("Failed to parse: invalid indices. It should be greater than zero");
      }

      result.add(new NMFData(rowIndex, parseColumns(split[1].trim()), modelGenerator.createRandomVector()));
    }
    return result;
  }
}
