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
package edu.snu.cay.dolphin.async.jobserver;

import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.webserver.HttpServer;

import javax.inject.Inject;

/**
 * Information class of Http Connection.
 */
final class HttpServerInfo {

  private final InjectionFuture<LocalAddressProvider> localAddressProviderFuture;
  private final InjectionFuture<HttpServer> httpServerFuture;

  @Inject
  private HttpServerInfo(final InjectionFuture<LocalAddressProvider> localAddressProviderFuture,
                         final InjectionFuture<HttpServer> httpServerFuture) {
    this.localAddressProviderFuture = localAddressProviderFuture;
    this.httpServerFuture = httpServerFuture;
  }

  String getLocalAddress() {
    return localAddressProviderFuture.get().getLocalAddress();
  }

  int getPort() {
    return httpServerFuture.get().getPort();
  }
}
