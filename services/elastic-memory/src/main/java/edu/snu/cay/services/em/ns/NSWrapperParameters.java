/**
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

package edu.snu.cay.services.em.ns;

import org.apache.reef.io.network.TransportFactory;
import org.apache.reef.io.network.impl.MessagingTransportFactory;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

/**
 * Named parameters for NSWrapper
 */
public final class NSWrapperParameters {

  @NamedParameter(doc = "Network service codec for NSWrapper")
  public static class NetworkServiceCodec implements Name<Codec<?>> {
  }

  @NamedParameter(doc = "Network message receive handler for NSWrapper")
  public static class NetworkServiceHandler implements Name<EventHandler<?>> {
  }

  @NamedParameter(doc = "Network exception handler for NSWrapper")
  public static class NetworkServiceExceptionHandler implements Name<EventHandler<?>> {
  }

  @NamedParameter(doc = "Network service port number for NSWrapper")
  public static class NetworkServicePort implements Name<Integer> {
  }

  @NamedParameter(doc = "Identifier factory for NSWrapper", default_class = StringIdentifierFactory.class)
  public static class NetworkServiceIdentifierFactory implements Name<IdentifierFactory> {
  }

  @NamedParameter(doc = "Transport factory for NSWrapper", default_class = MessagingTransportFactory.class)
  public static class NetworkServiceTransportFactory implements Name<TransportFactory> {
  }

  @NamedParameter(doc = "Name server address for NSWrapper")
  public static class NameServerAddr implements Name<String> {
  }

  @NamedParameter(doc = "Name server port for NSWrapper")
  public static class NameServerPort implements Name<Integer> {
  }
}
