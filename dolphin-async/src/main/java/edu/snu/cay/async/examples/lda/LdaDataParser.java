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
package edu.snu.cay.async.examples.lda;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

final class LdaDataParser {

  private static final Logger LOG = Logger.getLogger(LdaDataParser.class.getName());

  private final DataSet<LongWritable, Text> dataSet;
  private final int numTopics;

  @Inject
  private LdaDataParser(final DataSet<LongWritable, Text> dataSet,
                        @Parameter(LdaREEF.NumTopics.class) final int numTopics) {
    this.dataSet = dataSet;
    this.numTopics = numTopics;
  }

  List<Document> parse() {
    final List<Document> documents = new LinkedList<>();
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      final String text = keyValue.getSecond().toString().trim();
      if (text.startsWith("#") || text.length() == 0) {
        continue;
      }

      final String[] split = text.split("\\s+");

      final int[] words = new int[split.length];
      for (int i = 0; i < split.length; i++) {
        words[i] = Integer.parseInt(split[i]);
      }

      documents.add(new Document(words, numTopics));
    }

    LOG.log(Level.INFO, "Parsed {0} lines", documents.size());

    return documents;
  }
}
