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
package edu.snu.cay.services.em.driver;

import edu.snu.cay.services.em.driver.api.EMMaster;
import edu.snu.cay.services.em.ns.EMNetworkSetup;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;

/**
 * Wrapper object that is responsible to instantiate EMMaster in Driver.
 */
@DriverSide
public final class EMWrapper {
  private final EMMaster emMaster;
  private final EMConfProvider confProvider;
  private final EMNetworkSetup networkSetup;

  /**
   * Inject the instances.
   */
  @Inject
  public EMWrapper(final EMMaster emMaster,
                   final EMConfProvider confProvider,
                   final EMNetworkSetup networkSetup) throws InjectionException {
    this.emMaster = emMaster;
    this.confProvider = confProvider;
    this.networkSetup = networkSetup;
  }

  /**
   * @return The instance of EMMaster, which is an endpoint for Optimizer.
   */
  public EMMaster getInstance() {
    return emMaster;
  }

  /**
   * @return The helper class for configuration in EM.
   */
  public EMConfProvider getConfProvider() {
    return confProvider;
  }

  /**
   * @return The helper class for the network setup in EM.
   */
  public EMNetworkSetup getNetworkSetup() {
    return networkSetup;
  }
}
