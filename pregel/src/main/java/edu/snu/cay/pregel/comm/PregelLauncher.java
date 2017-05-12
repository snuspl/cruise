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
package edu.snu.cay.pregel.comm;


import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launcher for Pregel application.
 */

public final class PregelLauncher {

  private static final Logger LOG = Logger.getLogger(PregelLauncher.class.getName());

  private static final int JOB_TIMEOUT = 300000;
  private static final int MAX_NUMBER_OF_EVALUATORS = 2;
  private static final String DRIVER_IDENTIFIER = "Pregel";

  private PregelLauncher() {

  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final LauncherStatus status = launch();
    LOG.log(Level.INFO, "Pregel job completed: {0}", status);
  }

  public static LauncherStatus launch() throws InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(PregelMaster.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_IDENTIFIER)
        .set(DriverConfiguration.ON_DRIVER_STARTED, PregelMaster.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, PregelMaster.EvaluatorAllocatedHandler.class)
        .build();

    return DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, JOB_TIMEOUT);
  }

}
