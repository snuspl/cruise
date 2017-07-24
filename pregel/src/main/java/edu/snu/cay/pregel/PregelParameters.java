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

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Pregel specific parameters.
 */
public final class PregelParameters {

  @NamedParameter(doc = "configuration for worker task class, serialized as a string")
  public final class SerializedTaskConf implements Name<String> {

  }

  @NamedParameter(doc = "configuration for table class, serialized as a string")
  public final class SerializedMasterConf implements Name<String> {

  }

  @NamedParameter(doc = "The codec class for encoding and decoding message objects")
  public final class MessageCodec implements Name<Codec> {

  }

  @NamedParameter(doc = "The codec class for encoding and decoding vertex values")
  public final class VertexValueCodec implements Name<StreamingCodec> {

  }

  @NamedParameter(doc = "The codec class for encoding and decoding edge values")
  public final class EdgeCodec implements Name<StreamingCodec> {

  }

  @NamedParameter(doc = "Path of a input file", short_name = "input_path")
  public final class InputPath implements Name<String> {

  }
}
