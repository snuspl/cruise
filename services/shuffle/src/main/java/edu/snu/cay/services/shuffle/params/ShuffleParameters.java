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
package edu.snu.cay.services.shuffle.params;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.remote.Codec;

import java.util.Set;

/**
 * Parameters for shuffle service.
 */
public final class ShuffleParameters {

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private ShuffleParameters() {
  }

  /**
   * NetworkConnectionFactory identifier for ShuffleTupleMessage.
   */
  public static final String SHUFFLE_TUPLE_MSG_NETWORK_ID = "SHUFFLE_TUPLE_MSG_NETWORK_ID";

  /**
   * NetworkConnectionFactory identifier for ShuffleControlMessage.
   */
  public static final String SHUFFLE_CONTROL_MSG_NETWORK_ID = "SHUFFLE_CONTROL_MSG_NETWORK_ID";

  /**
   * Local end point id for driver.
   */
  public static final String SHUFFLE_DRIVER_LOCAL_END_POINT_ID = "SHUFFLE_DRIVER_LOCAL_END_POINT_ID";

  @NamedParameter(doc = "the name of ShuffleManger class")
  public static final class ShuffleManagerClassName implements Name<String> {
  }

  @NamedParameter(doc = "the key codec for TupleCodec")
  public static final class TupleKeyCodec implements Name<Codec> {
  }

  @NamedParameter(doc = "the value codec for TupleCodec")
  public static final class TupleValueCodec implements Name<Codec> {
  }

  @NamedParameter(doc = "name of the serialized shuffle description")
  public static final class ShuffleName implements Name<String> {
  }

  @NamedParameter(doc = "set of receiver identifiers of the shuffle description")
  public final class ShuffleReceiverIdSet implements Name<Set<String>> {
  }

  @NamedParameter(doc = "set of sender identifiers of the shuffle description")
  public final class ShuffleSenderIdSet implements Name<Set<String>> {
  }

  @NamedParameter(doc = "name of the ShuffleStrategy class of the shuffle description")
  public final class ShuffleStrategyClassName implements Name<String> {
  }

  @NamedParameter(doc = "name of the key codec class of the shuffle description")
  public static final class ShuffleKeyCodecClassName implements Name<String> {
  }

  @NamedParameter(doc = "name of the value codec class of the shuffle description")
  public final class ShuffleValueCodecClassName implements Name<String> {
  }

  @NamedParameter(doc = "set of serialized shuffle description")
  public static final class SerializedShuffleSet implements Name<Set<String>> {
  }

  @NamedParameter(doc = "the end point identifier")
  public static final class EndPointId implements Name<String> {
  }
}
