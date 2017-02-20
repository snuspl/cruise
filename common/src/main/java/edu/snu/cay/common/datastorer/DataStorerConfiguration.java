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
package edu.snu.cay.common.datastorer;

import edu.snu.cay.common.datastorer.param.BaseDir;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Builds configuration for DataStorer service.
 */
public final class DataStorerConfiguration extends ConfigurationModuleBuilder {

  /**
   * The base directory of the files.
   */
  public static final RequiredParameter<String> BASE_DIR = new RequiredParameter<>();

  /**
   * The implementation of {@link DataStorer}.
   */
  public static final RequiredImpl<DataStorer> DATA_STORER = new RequiredImpl<>();

  public static final ConfigurationModule CONF = new DataStorerConfiguration()
      .bindNamedParameter(BaseDir.class, BASE_DIR)
      .bindImplementation(DataStorer.class, DATA_STORER)
      .build();
}
