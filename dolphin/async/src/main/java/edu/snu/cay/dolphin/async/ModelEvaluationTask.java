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

import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * REEF Task for evaluating a model trained by training tasks.
 */
final class ModelEvaluationTask implements Task {
  private static final Logger LOG = Logger.getLogger(ModelEvaluationTask.class.getName());

  private final ModelEvaluator modelEvaluator;
  private final Trainer trainer;

  @Inject
  private ModelEvaluationTask(final ModelEvaluator modelEvaluator,
                              final Trainer trainer) {
    this.modelEvaluator = modelEvaluator;
    this.trainer = trainer;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    // evaluate all check-pointed models
    modelEvaluator.evaluate(trainer);

    return null;
  }
}
