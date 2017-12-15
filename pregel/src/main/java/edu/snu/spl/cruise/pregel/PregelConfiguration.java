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
package edu.snu.spl.cruise.pregel;

import edu.snu.spl.cruise.pregel.graph.api.Computation;
import edu.snu.spl.cruise.services.et.evaluator.api.DataParser;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;

import java.util.ArrayList;
import java.util.List;

/**
 * Job configuration of a Pregel on ET application.
 *
 * Call {@code newBuilder} and supply classes for {@link Computation},
 * {@link DataParser} and {@link Codec}s.
 * Use with {@link PregelLauncher#launch(String, String[], PregelConfiguration)} to launch application.
 */
@ClientSide
public final class PregelConfiguration {

  private final Class<? extends Computation> computationClass;

  private final Class<? extends StreamingCodec> vertexValueCodecClass;
  private final Class<? extends StreamingCodec> edgeCodecClass;
  private final Class<? extends DataParser> dataParserClass;

  private final Class<? extends StreamingCodec> messageValueCodecClass;
  private final List<Class<? extends Name<?>>> userParamList;

  private PregelConfiguration(final Class<? extends Computation> computationClass,
                              final Class<? extends StreamingCodec> vertexValueCodecClass,
                              final Class<? extends StreamingCodec> edgeCodecClass,
                              final Class<? extends DataParser> dataParserClass,
                              final Class<? extends StreamingCodec> messageValueCodecClass,
                              final List<Class<? extends Name<?>>> userParamList) {
    this.computationClass = computationClass;
    this.vertexValueCodecClass = vertexValueCodecClass;
    this.edgeCodecClass = edgeCodecClass;
    this.dataParserClass = dataParserClass;
    this.messageValueCodecClass = messageValueCodecClass;
    this.userParamList = userParamList;
  }

  public Class<? extends Computation> getComputationClass() {
    return computationClass;
  }

  public Class<? extends StreamingCodec> getVertexValueCodecClass() {
    return vertexValueCodecClass;
  }

  public Class<? extends StreamingCodec> getEdgeCodecClass() {
    return edgeCodecClass;
  }

  public Class<? extends DataParser> getDataParserClass() {
    return dataParserClass;
  }

  public Class<? extends StreamingCodec> getMessageValueCodecClass() {
    return messageValueCodecClass;
  }

  public List<Class<? extends Name<?>>> getUserParamList() {
    return userParamList;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder implements org.apache.reef.util.Builder<PregelConfiguration> {

    private Class<? extends Computation> computationClass;
    private Class<? extends StreamingCodec> vertexValueCodecClass;
    private Class<? extends StreamingCodec> edgeCodecClass;

    private Class<? extends DataParser> dataParserClass;

    private Class<? extends StreamingCodec> messageValueCodecClass;
    private List<Class<? extends Name<?>>> userParamList = new ArrayList<>();

    public Builder setComputationClass(final Class<? extends Computation> computationClass) {
      this.computationClass = computationClass;
      return this;
    }

    public Builder setVertexValueCodecClass(final Class<? extends StreamingCodec> vertexValueCodecClass) {
      this.vertexValueCodecClass = vertexValueCodecClass;
      return this;
    }

    public Builder setEdgeCodecClass(final Class<? extends StreamingCodec> edgeCodecClass) {
      this.edgeCodecClass = edgeCodecClass;
      return this;
    }

    public Builder setDataParserClass(final Class<? extends DataParser> dataParserClass) {
      this.dataParserClass = dataParserClass;
      return this;
    }

    public Builder setMessageValueCodecClass(final Class<? extends StreamingCodec> messageValueCodecClass) {
      this.messageValueCodecClass = messageValueCodecClass;
      return this;
    }

    public Builder addParameterClass(final Class<? extends Name<?>> parameterClass) {
      userParamList.add(parameterClass);
      return this;
    }

    @Override
    public PregelConfiguration build() {
      return new PregelConfiguration(computationClass, vertexValueCodecClass, edgeCodecClass, dataParserClass,
          messageValueCodecClass, userParamList);
    }
  }
}
