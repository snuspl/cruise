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

import edu.snu.cay.services.et.evaluator.api.DataParser;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Data parser that mimics data loading in Dolphin Async examples.
 * Random integers as many as {@link ExampleParameters.NumTrainingData} are assigned to each Worker.
 */
public final class ExampleDataParser implements DataParser<Integer> {
  private static final Random RAND = new Random();
  private final int numTrainingDataInstance;

  @Inject
  private ExampleDataParser(@Parameter(ExampleParameters.NumTrainingData.class) final int numTrainingDataInstances) {
    this.numTrainingDataInstance = numTrainingDataInstances;
  }

  @Override
  public List<Integer> parse(final Collection<String> rawData) {
    final List<Integer> list = new ArrayList<>(numTrainingDataInstance);
    for (int i = 0; i < numTrainingDataInstance; i++) {
      list.add(RAND.nextInt());
    }
    return list;
  }
}
