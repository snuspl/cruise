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
package edu.snu.cay.services.rrm;

import edu.snu.cay.services.rrm.impl.ResourceRequestManagerImpl;
import org.apache.reef.driver.parameters.EvaluatorAllocatedHandlers;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

/**
 * Configuration class for setting driver configurations of ResourceManager.
 */
public final class ResourceManagerConfiguration {

  @Inject
  private ResourceManagerConfiguration() {

  }

  /**
   * Configuration for REEF driver when using Resource Manager service.
   * Binds {@code RegisterEvaluatorHandler}.
   * @return configuration that should be submitted with a DriverConfiguration
   */
  public static Configuration getDriverConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(EvaluatorAllocatedHandlers.class, ResourceRequestManagerImpl.RegisterEvaluatorHandler.class)
        .build();
  }
}
