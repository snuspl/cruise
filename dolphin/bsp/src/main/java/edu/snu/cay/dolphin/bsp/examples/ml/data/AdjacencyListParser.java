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
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Adjacency List Parser class.
 * Assume that each line starts with a node id followed by its neighbor id set.
 */
public final class AdjacencyListParser implements DataParser<Map<Integer, List<Integer>>> {
  private static final Logger LOG = Logger.getLogger(AdjacencyListParser.class.getName());

  private final DataSet<LongWritable, Text> dataSet;
  private Map<Integer, List<Integer>> result = new HashMap<>();
  private ParseException parseException;

  @Inject
  public AdjacencyListParser(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public Map<Integer, List<Integer>> get() throws ParseException {
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
    final Map<Integer, List<Integer>> subgraphs = new HashMap<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      final String text = keyValue.getSecond().toString().trim();
      if (text.startsWith("#") || 0 == text.length()) {
        continue;
      }

      final String[] split = text.split("\\s+");
      final List<Integer> outdegree = new ArrayList<>(split.length - 1);
      try {
        final int nodeId = Integer.valueOf(split[0]);
        for (int i = 1; i < split.length; i++) {
          outdegree.add(Integer.valueOf(split[i]));
        }

        if (subgraphs.containsKey(nodeId)) {
          subgraphs.get(nodeId).addAll(outdegree);
        } else {
          subgraphs.put(nodeId, outdegree);
        }
      } catch (final NumberFormatException e) {
        parseException = new ParseException("Parse failed: each field should be a number");
        return;
      }
    }

    result = subgraphs;
  }
}
