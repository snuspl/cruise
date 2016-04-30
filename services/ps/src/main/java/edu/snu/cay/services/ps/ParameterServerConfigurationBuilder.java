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
package edu.snu.cay.services.ps;

import edu.snu.cay.services.ps.ParameterServerParameters.*;
import edu.snu.cay.services.ps.driver.api.ParameterServerManager;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.server.partitioned.PartitionedParameterServer;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Builder;

public final class ParameterServerConfigurationBuilder implements Builder<Configuration> {

  private static final ConfigurationSerializer CONFIGURATION_SERIALIZER = new AvroConfigurationSerializer();

  private Class<? extends ParameterServerManager> managerClass;
  private Class<? extends ParameterUpdater> updaterClass;
  private Class<? extends Codec> keyCodecClass = SerializableCodec.class;
  private Class<? extends Codec> preValueCodecClass = SerializableCodec.class;
  private Class<? extends Codec> valueCodecClass = SerializableCodec.class;

  public static ParameterServerConfigurationBuilder newBuilder() {
    return new ParameterServerConfigurationBuilder();
  }

  public ParameterServerConfigurationBuilder setManagerClass(
      final Class<? extends ParameterServerManager> managerClass) {
    this.managerClass = managerClass;
    return this;
  }

  public ParameterServerConfigurationBuilder setUpdaterClass(final Class<? extends ParameterUpdater> updaterClass) {
    this.updaterClass = updaterClass;
    return this;
  }

  public ParameterServerConfigurationBuilder setKeyCodecClass(final Class<? extends Codec> keyCodecClass) {
    this.keyCodecClass = keyCodecClass;
    return this;
  }

  public ParameterServerConfigurationBuilder setPreValueCodecClass(final Class<? extends Codec> preValueCodecClass) {
    this.preValueCodecClass = preValueCodecClass;
    return this;
  }

  public ParameterServerConfigurationBuilder setValueCodecClass(final Class<? extends Codec> valueCodecClass) {
    this.valueCodecClass = valueCodecClass;
    return this;
  }

  public Configuration build() {
    if (managerClass == null) {
      throw new RuntimeException("Manager class is required.");
    }

    if (updaterClass == null) {
      throw new RuntimeException("Updater class is required.");
    }

    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(ParameterServerManager.class, managerClass)
        .bindNamedParameter(SerializedUpdaterConfiguration.class,
            CONFIGURATION_SERIALIZER.toString(
                Tang.Factory.getTang().newConfigurationBuilder()
                    .bindImplementation(ParameterUpdater.class, updaterClass)
                    .build()))
        .bindNamedParameter(SerializedCodecConfiguration.class,
            CONFIGURATION_SERIALIZER.toString(
                Tang.Factory.getTang().newConfigurationBuilder()
                    .bindNamedParameter(KeyCodecName.class, keyCodecClass)
                    .bindNamedParameter(PreValueCodecName.class, preValueCodecClass)
                    .bindNamedParameter(ValueCodecName.class, valueCodecClass)
                    .build()))
        .build();
  }
}
