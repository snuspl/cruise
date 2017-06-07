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
package edu.snu.cay.services.et.examples.load;

import com.google.common.collect.Lists;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Data parser class for key and value which type is String.
 * Each line has the following form:
 *
 * <p>Key(Long) Value(String)</p>
 */
public final class KVDataParser implements DataParser<Pair<Long, String>> {

  @Inject
  private KVDataParser() {

  }

  @Override
  public List<Pair<Long, String>> parse(final Collection<String> rawData) {

    final List<Pair<Long, String>> parsedList = new ArrayList<>();
    for (final String line : rawData) {

      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }
      final List<String> lineDataList = Lists.newArrayList(line.split(" "));
      parsedList.add(Pair.of(Long.parseLong(lineDataList.get(0)), lineDataList.get(1)));
    }

    return parsedList;
  }
}
