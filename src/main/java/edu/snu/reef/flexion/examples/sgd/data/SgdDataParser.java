/**
 * Copyright (C) 2014 Seoul National University
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
package edu.snu.reef.flexion.examples.sgd.data;

import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SgdDataParser implements DataParser<List<Example>> {
  private final static Logger LOG = Logger.getLogger(SgdDataParser.class.getName());
  private final AtomicInteger count = new AtomicInteger(0);

  private final DataSet<LongWritable, Text> dataSet;
  private List<Example> result;
  private ParseException parseException;

  @Inject
  public SgdDataParser(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public final List<Example> get() throws ParseException {
    LOG.log(Level.INFO, "SgdDataParser called {0} times", count.incrementAndGet());
    if (result == null) {
      parse();
    }

    if (parseException != null) {
      throw parseException;
    }

    return result;
  }

  @Override
  public final void parse() {
    LOG.log(Level.INFO, "Trying to parse!");
    result = new ArrayList<>();

    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      String[] split = keyValue.second.toString().trim().split(" ");
      if (split.length <= 1) {
        parseException = new ParseException("Parsed failed: need label AND data");
        return;
      }

     double label;
     final Vector data = new DenseVector(split.length);
     try {
       label = Double.valueOf(split[split.length - 1]);
       for (int i = 0; i < split.length - 1; i++) {
         data.set(i, Double.valueOf(split[i]));
       }
       data.set(data.size() - 1, 1);

     } catch (final NumberFormatException e) {
       parseException = new ParseException("Parse failed: numbers should be DOUBLE");
       return;
     }

     result.add(new Example(label, data));
    }
  }
}
