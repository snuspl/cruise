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

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Optional;

/**
 * ModelAccessor that provides model assigned to Trainer threads locally.
 */
@ThreadSafe
public final class ThreadLocalModelAccessor<M extends Copyable<M>> implements ModelAccessor<M> {
  private ThreadLocal<M> model;

  @Inject
  private ThreadLocalModelAccessor() {
  }

  @Override
  public void resetModel(final M newModel) {
    final M copied = newModel.copyOf(); // we keep the model's copy to discard the changes made outside.
    model = ThreadLocal.withInitial(copied::copyOf);
  }

  @Override
  public Optional<M> getModel() {
    if (model == null) {
      return Optional.empty();
    } else {
      return Optional.ofNullable(model.get());
    }
  }
}
