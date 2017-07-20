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
package edu.snu.cay.pregel;

import edu.snu.cay.pregel.common.DefaultVertexCodec;
import edu.snu.cay.pregel.graph.api.Computation;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.io.serialization.Codec;

/**
 * Job configuration of a Pregel on ET application.
 *
 * Call {@code newBuilder} and supply classes for {@link Computation}, {@link UpdateFunction},
 * {@link DataParser} and codecs
 * Use with {@link PregelLauncher#launch(String[], PregelConfiguration)} to launch application.
 */
@ClientSide
public final class PregelConfiguration {

  private final Class<? extends Computation> computationClass;

  private final Class<? extends Codec> vertexCodecClass;
  private final Class<? extends DataParser> dataParserClass;

  private final Class<? extends Codec> messageCodecClass;
  private final Class<? extends UpdateFunction> messageUpdateFunctionClass;

  private PregelConfiguration(final Builder builder) {
    this.computationClass = builder.getComputationClass();
    this.vertexCodecClass = builder.getVertexCodecClass();
    this.dataParserClass = builder.getDataParserClass();
    this.messageCodecClass = builder.getMessageCodecClass();
    this.messageUpdateFunctionClass = builder.getMessageUpdateFunctionClass();
  }

  public Class<? extends Computation> getComputationClass() {
    return computationClass;
  }

  public Class<? extends Codec> getVertexCodecClass() {
    return vertexCodecClass;
  }

  public Class<? extends DataParser> getDataParserClass() {
    return dataParserClass;
  }

  public Class<? extends Codec> getMessageCodecClass() {
    return messageCodecClass;
  }

  public Class<? extends UpdateFunction> getMessageUpdateFunctionClass() {
    return messageUpdateFunctionClass;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder implements org.apache.reef.util.Builder<PregelConfiguration> {

    private Class<? extends Computation> computationClass;
    private Class<? extends Codec> vertexCodecClass = DefaultVertexCodec.class;
    private Class<? extends DataParser> dataParserClass;

    private Class<? extends Codec> messageCodecClass;
    private Class<? extends UpdateFunction> messageUpdateFunctionClass;


    public Builder setComputationClass(final Class<? extends Computation> computationClass) {
      this.computationClass = computationClass;
      return this;
    }

    public Builder setVertexCodecClass(final Class<? extends Codec> vertexCodecClass) {
      this.vertexCodecClass = vertexCodecClass;
      return this;
    }

    public Builder setDataParserClass(final Class<? extends DataParser> dataParserClass) {
      this.dataParserClass = dataParserClass;
      return this;
    }

    public Builder setMessageCodecClass(final Class<? extends Codec> messageCodecClass) {
      this.messageCodecClass = messageCodecClass;
      return this;
    }

    public Builder setMessageUpdateFunctionClass(final Class<? extends UpdateFunction> updateFunctionClass) {
      this.messageUpdateFunctionClass = updateFunctionClass;
      return this;
    }

    private Class<? extends Computation> getComputationClass() {
      return computationClass;
    }

    private Class<? extends Codec> getVertexCodecClass() {
      return vertexCodecClass;
    }

    private Class<? extends DataParser> getDataParserClass() {
      return dataParserClass;
    }

    private Class<? extends Codec> getMessageCodecClass() {
      return messageCodecClass;
    }

    private Class<? extends UpdateFunction> getMessageUpdateFunctionClass() {
      return messageUpdateFunctionClass;
    }

    @Override
    public PregelConfiguration build() {
      return new PregelConfiguration(this);
    }
  }
}
