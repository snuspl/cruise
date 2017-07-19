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

import edu.snu.cay.pregel.graph.api.Computation;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.io.serialization.Codec;

/**
 * Created by cmslab on 7/19/17.
 */
@ClientSide
public class PregelConfiguration {

  private final Class<? extends Computation> computationClass;
  private final Class<? extends Codec> vertexCodecClass;
  private final Class<? extends Codec> messageCodecClass;

  private PregelConfiguration(final Builder builder) {
    this.computationClass = builder.getComputationClass();
    this.vertexCodecClass = builder.getVertexCodecClass();
    this.messageCodecClass = builder.getMessageCodecClass();
  }

  public Class<? extends Computation> getComputationClass() {
    return computationClass;
  }

  public Class<? extends Codec> getVertexCodecClass() {
    return vertexCodecClass;
  }

  public Class<? extends Codec> getMessageCodecClass() {
    return messageCodecClass;
  }

  public static class Builder implements org.apache.reef.util.Builder<PregelConfiguration> {

    private Class<? extends Computation> computationClass;
    private Class<? extends Codec> vertexCodecClass;
    private Class<? extends Codec> messageCodecClass;

    private static Builder newBuilder() {
      return new Builder();
    }

    public Builder setComputationClass(final Class<? extends Computation> computationClass) {
      this.computationClass = computationClass;
      return this;
    }

    public Builder setVertexCodecClass(final Class<? extends Codec> vertexCodecClass) {
      this.vertexCodecClass = vertexCodecClass;
      return this;
    }

    public Builder setMessageCodecClass(final Class<? extends Codec> messageCodecClass) {
      this.messageCodecClass = messageCodecClass;
      return this;
    }

    private Class<? extends Computation> getComputationClass() {
      return computationClass;
    }

    public Class<? extends Codec> getVertexCodecClass() {
      return vertexCodecClass;
    }

    public Class<? extends Codec> getMessageCodecClass() {
      return messageCodecClass;
    }

    @Override
    public PregelConfiguration build() {
      return new PregelConfiguration(this);
    }
  }
}
