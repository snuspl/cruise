/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.ps.driver.api;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;

/**
 * Driver-side manager for the Parameter Server.
 * Service configuration and PS-related evaluator management is done by this class.
 * Although public, the methods should not be called by the user directly because
 * {@code ParameterServerDriver} calls them internally.
 */
@DriverSide
public interface ParameterServerManager {

  /**
   * @return service configuration for an Evaluator that uses a {@code ParameterWorker}
   */
  Configuration getWorkerServiceConfiguration(String contextId);

  /**
   * @return service configuration for an Evaluator that uses a {@code ParameterServer}
   */
  Configuration getServerServiceConfiguration(String contextId);
}
