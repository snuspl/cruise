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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.evaluator.api.DataParser;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * DataParser that just returns rawData without any processing.
 */
public final class DefaultDataParser implements DataParser<String> {
  @Inject
  private DefaultDataParser() {
  }

  @Override
  public List<String> parse(final Collection<String> rawData) {

    final List<String> parsedList = new ArrayList<>();
    for (final String line : rawData) {

      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }
      parsedList.add(line);
    }
    return parsedList;
  }
}
