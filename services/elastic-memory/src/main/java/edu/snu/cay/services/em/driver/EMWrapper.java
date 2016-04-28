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
package edu.snu.cay.services.em.driver;

import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.ns.EMNetworkSetup;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;

/**
 * Wrapper object that is responsible to instantiate ElasticMemory in Driver.
 */
@DriverSide
public final class EMWrapper {
  private final ElasticMemory elasticMemory;
  private final ElasticMemoryConfiguration conf;
  private final EMNetworkSetup networkSetup;

  /**
   * Inject the instances.
   */
  public EMWrapper(final Injector injector) throws InjectionException {
    this.elasticMemory = injector.getInstance(ElasticMemory.class);
    this.conf = injector.getInstance(ElasticMemoryConfiguration.class);
    this.networkSetup = injector.getInstance(EMNetworkSetup.class);
  }

  /**
   * @return The instance of ElasticMemory, which is an endpoint for Optimizer.
   */
  public ElasticMemory getInstance() {
    return elasticMemory;
  }

  /**
   * @return The helper class for configuration in EM.
   */
  public ElasticMemoryConfiguration getConf() {
    return conf;
  }

  /**
   * @return The helper class for the network setup in EM.
   */
  public EMNetworkSetup getNetworkSetup() {
    return networkSetup;
  }
}
