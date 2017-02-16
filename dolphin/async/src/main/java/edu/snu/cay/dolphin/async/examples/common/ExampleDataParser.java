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
package edu.snu.cay.dolphin.async.examples.common;

import edu.snu.cay.dolphin.async.DataParser;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

/**
 * Data parser that mimics data loading in Dolphin Async examples.
 * Initialize the data as many as {@link ExampleParameters.NumTrainingData}
 */
public final class ExampleDataParser implements DataParser<Object> {
  private final int numTrainingDataInstance;

  @Inject
  private ExampleDataParser(@Parameter(ExampleParameters.NumTrainingData.class) final int numTrainingDataInstances) {
    this.numTrainingDataInstance = numTrainingDataInstances;
  }


  @Override
  public List<Object> parse() {
    final List<Object> list = new LinkedList<>();
    for (int i = 0; i < numTrainingDataInstance; i++) {
      list.add(i);
    }
    return list;
  }
}
