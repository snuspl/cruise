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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.utils.Copyable;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Optional;

/**
 * A class that holds local model shared by multiple trainer threads.
 * Trainer threads access the model through {@link #getModel()}.
 * @param <M> Type of the app-specific model.
 */
@DefaultImplementation(ThreadLocalModelHolder.class)
public interface ModelHolder<M extends Copyable<M>> {

  /**
   * Updates the model to the latest one.
   * @param model a new model
   */
  void resetModel(M model);

  /**
   * @return the up-to-date model if set through {@link #resetModel}. Otherwise {@link Optional#empty()} is returned.
   */
  Optional<M> getModel();
}
