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
package edu.snu.cay.dolphin.async.core.worker;

import edu.snu.cay.services.et.evaluator.api.Tasklet;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Logger;

/**
 * REEF Task for evaluating a model trained by training tasks.
 */
public final class ModelEvaluationTasklet implements Tasklet {
  private static final Logger LOG = Logger.getLogger(ModelEvaluationTasklet.class.getName());

  private final ModelEvaluator modelEvaluator;
  private final TestDataProvider testDataProvider;
  private final Trainer trainer;

  @Inject
  private ModelEvaluationTasklet(final ModelEvaluator modelEvaluator,
                                 final TestDataProvider testDataProvider,
                                 final Trainer trainer) {
    this.modelEvaluator = modelEvaluator;
    this.testDataProvider = testDataProvider;
    this.trainer = trainer;
  }

  @Override
  public void run() throws Exception {
    final List testData = testDataProvider.getTestData();

    // evaluate all check-pointed models
    modelEvaluator.evaluate(trainer, testData);
  }

  @Override
  public void close() {

  }
}
