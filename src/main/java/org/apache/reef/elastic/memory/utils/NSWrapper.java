/**
 * Copyright (C) 2014 Seoul National University
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

package org.apache.reef.elastic.memory.utils;

import org.apache.reef.io.network.group.impl.driver.ExceptionHandler;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.impl.NetworkServiceParameters;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;

/**
 * Wrapper class for NetworkService
 */
public final class NSWrapper<T> {

  private final NetworkService<T> networkService;
  private final NSWrapperMessageHandler networkServiceHandler;

  @Inject
  public NSWrapper(
      @Parameter(NSWrapperParameters.NameServerAddr.class) final String nameServerAddr,
      @Parameter(NSWrapperParameters.NameServerPort.class) final Integer nameServerPort) throws InjectionException {

    JavaConfigurationBuilder NSConfigurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    NSConfigurationBuilder.bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class,
        org.apache.reef.io.network.util.StringCodec.class);
    NSConfigurationBuilder.bindNamedParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class,
        ExceptionHandler.class);
    NSConfigurationBuilder.bindNamedParameter(NameServerParameters.NameServerAddr.class, nameServerAddr);
    NSConfigurationBuilder.bindNamedParameter(NameServerParameters.NameServerPort.class,
        Integer.toString(nameServerPort));
    NSConfigurationBuilder.bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, "0");
    networkServiceHandler = new NSWrapperMessageHandler();
    Injector networkServiceInjector = Tang.Factory.getTang().newInjector(NSConfigurationBuilder.build());

    networkServiceInjector
        .bindVolatileParameter(NetworkServiceParameters.NetworkServiceHandler.class, networkServiceHandler);
    networkService = networkServiceInjector.getInstance(NetworkService.class);
  }

  public NetworkService getNetworkService() {
    return networkService;
  }
  public NSWrapperMessageHandler getNetworkServiceHandler() {
    return networkServiceHandler;
  }
}
