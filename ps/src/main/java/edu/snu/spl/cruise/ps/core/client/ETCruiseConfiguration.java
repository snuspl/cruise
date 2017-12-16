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
package edu.snu.spl.cruise.ps.core.client;

import edu.snu.spl.cruise.ps.core.worker.Trainer;
import edu.snu.spl.cruise.services.et.evaluator.api.DataParser;
import edu.snu.spl.cruise.services.et.evaluator.api.UpdateFunction;
import edu.snu.spl.cruise.utils.StreamingSerializableCodec;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.util.BuilderUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * Job configuration of a Cruise on ET application.
 *
 * Call {@code newBuilder} and supply classes for {@link Trainer}, {@link UpdateFunction}, {@link DataParser}, codecs,
 * parameters, configuration for workers, and configuration for servers.
 * {@link SerializableCodec}s are used in case codec classes are not given. Parameter classes are also optional.
 * Use with {@link ETCruiseLauncher#launch(String, String[], ETCruiseConfiguration)} to launch application.
 */
@ClientSide
public final class ETCruiseConfiguration {
  private final Class<? extends Trainer> trainerClass;
  private final Class<? extends DataParser> inputParserClass;
  private final Class<? extends StreamingCodec> inputKeyCodecClass;
  private final Class<? extends StreamingCodec> inputValueCodecClass;
  private final Class<? extends UpdateFunction> modelUpdateFunctionClass;
  private final Class<? extends StreamingCodec> modelKeyCodecClass;
  private final Class<? extends StreamingCodec> modelValueCodecClass;
  private final Class<? extends Codec> modelUpdateValueCodecClass;

  private final List<Class<? extends Name<?>>> parameterClassList;
  private final Configuration workerConfiguration;
  private final Configuration serverConfiguration;

  private ETCruiseConfiguration(final Class<? extends Trainer> trainerClass,
                                 final Class<? extends DataParser> inputParserClass,
                                 final Class<? extends StreamingCodec> inputKeyCodecClass,
                                 final Class<? extends StreamingCodec> inputValueCodecClass,
                                 final Class<? extends UpdateFunction> modelUpdateFunctionClass,
                                 final Class<? extends StreamingCodec> modelKeyCodecClass,
                                 final Class<? extends StreamingCodec> modelValueCodecClass,
                                 final Class<? extends Codec> modelUpdateValueCodecClass,
                                 final List<Class<? extends Name<?>>> parameterClassList,
                                 final Configuration workerConfiguration,
                                 final Configuration serverConfiguration) {
    this.trainerClass = trainerClass;
    this.inputParserClass = inputParserClass;
    this.inputKeyCodecClass = inputKeyCodecClass;
    this.inputValueCodecClass = inputValueCodecClass;
    this.modelUpdateFunctionClass = modelUpdateFunctionClass;
    this.modelKeyCodecClass = modelKeyCodecClass;
    this.modelValueCodecClass = modelValueCodecClass;
    this.modelUpdateValueCodecClass = modelUpdateValueCodecClass;
    this.parameterClassList = parameterClassList;
    this.workerConfiguration = workerConfiguration;
    this.serverConfiguration = serverConfiguration;
  }

  public Class<? extends Trainer> getTrainerClass() {
    return trainerClass;
  }

  public Class<? extends DataParser> getInputParserClass() {
    return inputParserClass;
  }

  public Class<? extends StreamingCodec> getInputKeyCodecClass() {
    return inputKeyCodecClass;
  }

  public Class<? extends StreamingCodec> getInputValueCodecClass() {
    return inputValueCodecClass;
  }

  public Class<? extends UpdateFunction> getModelUpdateFunctionClass() {
    return modelUpdateFunctionClass;
  }

  public Class<? extends StreamingCodec> getModelKeyCodecClass() {
    return modelKeyCodecClass;
  }

  public Class<? extends StreamingCodec> getModelValueCodecClass() {
    return modelValueCodecClass;
  }

  public Class<? extends Codec> getModelUpdateValueCodecClass() {
    return modelUpdateValueCodecClass;
  }

  public List<Class<? extends Name<?>>> getParameterClassList() {
    return parameterClassList;
  }

  public Configuration getWorkerConfiguration() {
    return workerConfiguration;
  }

  public Configuration getServerConfiguration() {
    return serverConfiguration;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder implements org.apache.reef.util.Builder<ETCruiseConfiguration> {
    private Class<? extends Trainer> trainerClass;
    private Class<? extends DataParser> inputParserClass;
    private Class<? extends StreamingCodec> inputKeyCodecClass = StreamingSerializableCodec.class;
    private Class<? extends StreamingCodec> inputValueCodecClass = StreamingSerializableCodec.class;

    private Class<? extends UpdateFunction> modelUpdateFunctionClass;
    private Class<? extends StreamingCodec> modelKeyCodecClass = StreamingSerializableCodec.class;
    private Class<? extends StreamingCodec> modelValueCodecClass = StreamingSerializableCodec.class;
    private Class<? extends Codec> modelUpdateValueCodecClass = SerializableCodec.class;

    private List<Class<? extends Name<?>>> parameterClassList = new LinkedList<>();
    private Configuration workerConfiguration = Tang.Factory.getTang().newConfigurationBuilder().build();
    private Configuration serverConfiguration = Tang.Factory.getTang().newConfigurationBuilder().build();

    public Builder setTrainerClass(final Class<? extends Trainer> trainerClass) {
      this.trainerClass = trainerClass;
      return this;
    }

    public Builder setInputParserClass(final Class<? extends DataParser> inputParserClass) {
      this.inputParserClass = inputParserClass;
      return this;
    }

    public Builder setInputKeyCodecClass(final Class<? extends StreamingCodec> inputKeyCodecClass) {
      this.inputKeyCodecClass = inputKeyCodecClass;
      return this;
    }

    public Builder setInputValueCodecClass(final Class<? extends StreamingCodec> inputValueCodecClass) {
      this.inputValueCodecClass = inputValueCodecClass;
      return this;
    }

    public Builder setModelUpdateFunctionClass(final Class<? extends UpdateFunction> modelUpdateFunctionClass) {
      this.modelUpdateFunctionClass = modelUpdateFunctionClass;
      return this;
    }

    public Builder setModelKeyCodecClass(final Class<? extends StreamingCodec> modelKeyCodecClass) {
      this.modelKeyCodecClass = modelKeyCodecClass;
      return this;
    }

    public Builder setModelValueCodecClass(final Class<? extends StreamingCodec> modelValueCodecClass) {
      this.modelValueCodecClass = modelValueCodecClass;
      return this;
    }

    public Builder setModelUpdateValueCodecClass(final Class<? extends Codec> modelUpdateValueCodecClass) {
      this.modelUpdateValueCodecClass = modelUpdateValueCodecClass;
      return this;
    }

    public Builder addParameterClass(final Class<? extends Name<?>> parameterClass) {
      this.parameterClassList.add(parameterClass);
      return this;
    }

    public Builder setWorkerConfiguration(final Configuration workerConfiguration) {
      this.workerConfiguration = workerConfiguration;
      return this;
    }

    public Builder setServerConfiguration(final Configuration serverConfiguration) {
      this.serverConfiguration = serverConfiguration;
      return this;
    }

    @Override
    public ETCruiseConfiguration build() {
      BuilderUtils.notNull(trainerClass);
      BuilderUtils.notNull(inputParserClass);
      BuilderUtils.notNull(modelUpdateFunctionClass);

      return new ETCruiseConfiguration(trainerClass, inputParserClass, inputKeyCodecClass, inputValueCodecClass,
          modelUpdateFunctionClass, modelKeyCodecClass, modelValueCodecClass, modelUpdateValueCodecClass,
          parameterClassList, workerConfiguration, serverConfiguration);
    }
  }
}
