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
package edu.snu.cay.dolphin.core;


import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import java.util.logging.Logger;

/**
 * Key-value store service used to pass the current stage's result to the next stage.
 * Should be inserted alongside a context.
 */
public final class KeyValueStoreService {
  private static final Logger LOG = Logger.getLogger(KeyValueStoreService.class.getName());

  /**
   * Should not be instantiated.
   */
  private KeyValueStoreService() {
  }

  public static Configuration getServiceConfiguration() {
    final Configuration partialServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, KeyValueStore.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(partialServiceConf)
        .build();
  }
}
