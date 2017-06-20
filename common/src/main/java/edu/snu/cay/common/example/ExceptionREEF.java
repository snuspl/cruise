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
package edu.snu.cay.common.example;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A client code for Exception check app.
 * It checks that any exception thrown in user-thread can terminate REEF job
 * by using {@link edu.snu.cay.utils.CatchableExecutors}.
 */
public final class ExceptionREEF {

  private static final Logger LOG = Logger.getLogger(ExceptionREEF.class.getName());

  private static final int JOB_TIMEOUT = 20000; // 20 sec.

  private ExceptionREEF() {

  }

  private static final Configuration RUNTIME_CONFIG =
      LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 2)
          .build();

  private static final Configuration DRIVER_CONFIG =
      DriverConfiguration.CONF
          .set(DriverConfiguration.DRIVER_IDENTIFIER, "ExceptionREEF")
          .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(ExceptionREEFDriver.class))
          .set(DriverConfiguration.ON_DRIVER_STARTED, ExceptionREEFDriver.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, ExceptionREEFDriver.EvaluatorAllocatedHandler.class)
          .build();

  public static void main(final String[] args) throws InjectionException {

    final LauncherStatus status = DriverLauncher.getLauncher(RUNTIME_CONFIG)
        .run(DRIVER_CONFIG, JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
