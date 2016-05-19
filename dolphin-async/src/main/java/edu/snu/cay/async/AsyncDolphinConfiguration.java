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
package edu.snu.cay.async;

import edu.snu.cay.services.em.serialize.JavaSerializer;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Name;

import java.util.LinkedList;
import java.util.List;

/**
 * Job configuration of a {@code dolphin-async} application.
 *
 * Call {@code newBuilder} and supply classes for {@link Worker}, {@link ParameterUpdater}, codecs, and parameters.
 * {@link SerializableCodec}s are used in case codec classes are not given. Parameter classes are also optional.
 * Use with {@link AsyncDolphinLauncher#launch(String, String[], AsyncDolphinConfiguration)} to launch application.
 */
@ClientSide
public final class AsyncDolphinConfiguration {
  private final Class<? extends Worker> workerClass;
  private final Class<? extends ParameterUpdater> updaterClass;
  private final Class<? extends Codec> keyCodecClass;
  private final Class<? extends Codec> preValueCodecClass;
  private final Class<? extends Codec> valueCodecClass;
  private final List<Class<? extends Name<?>>> parameterClassList;
  private final Class<? extends Serializer> workerSerializerClass;
  private final Class<? extends Serializer> serverSerializerClass;


  private AsyncDolphinConfiguration(final Class<? extends Worker> workerClass,
                                    final Class<? extends ParameterUpdater> updaterClass,
                                    final Class<? extends Codec> keyCodecClass,
                                    final Class<? extends Codec> preValueCodecClass,
                                    final Class<? extends Codec> valueCodecClass,
                                    final List<Class<? extends Name<?>>> parameterClassList,
                                    final Class<? extends Serializer> workerSerializerClass,
                                    final Class<? extends Serializer> serverSerializerClass) {
    this.workerClass = workerClass;
    this.updaterClass = updaterClass;
    this.keyCodecClass = keyCodecClass;
    this.preValueCodecClass = preValueCodecClass;
    this.valueCodecClass = valueCodecClass;
    this.parameterClassList = parameterClassList;
    this.workerSerializerClass = workerSerializerClass;
    this.serverSerializerClass = serverSerializerClass;
  }

  public Class<? extends Worker> getWorkerClass() {
    return workerClass;
  }

  public Class<? extends ParameterUpdater> getUpdaterClass() {
    return updaterClass;
  }

  public Class<? extends Codec> getKeyCodecClass() {
    return keyCodecClass;
  }

  public Class<? extends Codec> getPreValueCodecClass() {
    return preValueCodecClass;
  }

  public Class<? extends Codec> getValueCodecClass() {
    return valueCodecClass;
  }

  public List<Class<? extends Name<?>>> getParameterClassList() {
    return parameterClassList;
  }

  public Class<? extends Serializer> getWorkerSerializerClass() {
    return workerSerializerClass;
  }

  public Class<? extends Serializer> getServerSerializerClass() {
    return serverSerializerClass;
  }


  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder implements org.apache.reef.util.Builder<AsyncDolphinConfiguration> {
    private Class<? extends Worker> workerClass;
    private Class<? extends ParameterUpdater> updaterClass;
    private Class<? extends Codec> keyCodecClass = SerializableCodec.class;
    private Class<? extends Codec> preValueCodecClass = SerializableCodec.class;
    private Class<? extends Codec> valueCodecClass = SerializableCodec.class;
    private List<Class<? extends Name<?>>> parameterClassList = new LinkedList<>();
    private Class<? extends Serializer> workerSerializerClass = JavaSerializer.class;
    private Class<? extends Serializer> serverSerializerClass = JavaSerializer.class;


    public Builder setWorkerClass(final Class<? extends Worker> workerClass) {
      this.workerClass = workerClass;
      return this;
    }

    public Builder setUpdaterClass(final Class<? extends ParameterUpdater> updaterClass) {
      this.updaterClass = updaterClass;
      return this;
    }

    public Builder setKeyCodecClass(final Class<? extends Codec> keyCodecClass) {
      this.keyCodecClass = keyCodecClass;
      return this;
    }

    public Builder setPreValueCodecClass(final Class<? extends Codec> preValueCodecClass) {
      this.preValueCodecClass = preValueCodecClass;
      return this;
    }

    public Builder setValueCodecClass(final Class<? extends Codec> valueCodecClass) {
      this.valueCodecClass = valueCodecClass;
      return this;
    }

    public Builder addParameterClass(final Class<? extends Name<?>> parameterClass) {
      this.parameterClassList.add(parameterClass);
      return this;
    }

    public Builder setWorkerSerializerClass(final Class<? extends Serializer> workerSerializerClass) {
      this.workerSerializerClass = workerSerializerClass;
      return this;
    }

    public Builder setServerSerializerClass(final Class<? extends Serializer> serverSerializerClass) {
      this.serverSerializerClass = serverSerializerClass;
      return this;
    }

    @Override
    public AsyncDolphinConfiguration build() {
      if (workerClass == null) {
        throw new RuntimeException("Worker class is required.");
      }

      if (updaterClass == null) {
        throw new RuntimeException("Updater class is required.");
      }

      return new AsyncDolphinConfiguration(workerClass, updaterClass,
          keyCodecClass, preValueCodecClass, valueCodecClass, parameterClassList,
          workerSerializerClass, serverSerializerClass);
    }
  }
}
