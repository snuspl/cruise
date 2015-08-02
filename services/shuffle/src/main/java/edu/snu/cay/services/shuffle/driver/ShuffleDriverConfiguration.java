/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cay.services.shuffle.driver;

import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Configuration module for driver-side shuffle components.
 */
public final class ShuffleDriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * The name of shuffle manager class.
   */
  public static final RequiredParameter<String> SHUFFLE_MANAGER_CLASS_NAME = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new ShuffleDriverConfiguration()
      .bindNamedParameter(ShuffleParameters.ShuffleManagerClassName.class, SHUFFLE_MANAGER_CLASS_NAME)
      .build();
}
