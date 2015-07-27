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
package edu.snu.cay.services.shuffle.utils;

import org.apache.reef.io.network.naming.NameClient;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;

/**
 * NameResolverWrapper for NetworkConnectionService in driver.
 * There is no way to bind current local address as NameResolver's name server address
 * and current port number of name server as NameResolver's name server port.
 * The wrapper manually create name client which has the parameters with local values.
 *
 * This class will be removed after REEF-474 is resolved.
 */
public final class NameResolverWrapper implements NameResolver {

  private final NameClient nameClient;

  @Inject
  public NameResolverWrapper(
      final NameServer nameServer,
      final LocalAddressProvider localAddressProvider,
      final TransportFactory tpFactory) {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(LocalAddressProvider.class, localAddressProvider);
    injector.bindVolatileInstance(TransportFactory.class, tpFactory);
    injector.bindVolatileParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress());
    injector.bindVolatileParameter(NameResolverNameServerPort.class, nameServer.getPort());
    try {
      nameClient = injector.getInstance(NameClient.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    nameClient.close();
  }

  @Override
  public InetSocketAddress lookup(final Identifier id) throws Exception {
    return nameClient.lookup(id);
  }

  @Override
  public void register(final Identifier id, final InetSocketAddress addr) throws Exception {
    nameClient.register(id, addr);
  }

  @Override
  public void unregister(final Identifier id) throws Exception {
    nameClient.unregister(id);
  }
}
