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
package edu.snu.cay.async.examples.addinteger;

import edu.snu.cay.async.Worker;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Worker} class for the AddIntegerREEF application.
 * Pushes a value to the server and checks the current value at the server via pull, once per iteration.
 */
final class AddIntegerWorker implements Worker {
  private static final Logger LOG = Logger.getLogger(AddIntegerWorker.class.getName());
  private static final int KEY = 0;

  private final ParameterWorker<Integer, Integer, Integer> parameterWorker;
  private final int parameter;

  @Inject
  private AddIntegerWorker(final ParameterWorker<Integer, Integer, Integer> parameterWorker,
                           @Parameter(AddIntegerREEF.AddIntegerParameter.class) final int parameter) {
    this.parameterWorker = parameterWorker;
    this.parameter = parameter;
  }

  @Override
  public void initialize() {
  }

  @Override
  public void run() {
    parameterWorker.push(KEY, parameter);
    LOG.log(Level.INFO, "Current value associated with key {0} is {1}", new Object[]{KEY, parameterWorker.pull(KEY)});
  }

  @Override
  public void cleanup() {
  }
}
