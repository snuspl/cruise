/**
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
package edu.snu.reef.dolphin.examples.simple;

import edu.snu.reef.dolphin.core.DataParser;
import edu.snu.reef.dolphin.core.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class SimpleDataParser implements DataParser<List<String>> {
  private final static Logger LOG = Logger.getLogger(SimpleDataParser.class.getName());

  private final DataSet<LongWritable, Text> dataSet;
  private List<String> result;

  @Inject
  public SimpleDataParser(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public final List<String> get() throws ParseException {
    if (result == null) {
      parse();
    }
    return result;
  }

  @Override
  public final void parse() {
    final List<String> texts = new LinkedList<>();
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      texts.add(keyValue.second.toString());
    }
    result = texts;
  }
}
