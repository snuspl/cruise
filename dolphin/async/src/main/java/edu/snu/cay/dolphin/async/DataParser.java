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
package edu.snu.cay.dolphin.async;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Parses training data in the app-specific format. If the application does not need any parser (e.g., examples),
 * then the parser class should be set as {@link NullDataParser} in {@link AsyncDolphinConfiguration}.
 * @param <T> Type of the training data instances.
 */
@EvaluatorSide
@DefaultImplementation(NullDataParser.class)
public interface DataParser<T> {

  /**
   * @return the list of training data
   */
  List<T> parse();
}
