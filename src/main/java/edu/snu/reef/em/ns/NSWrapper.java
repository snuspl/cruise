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

package edu.snu.reef.em.ns;

import edu.snu.reef.em.msg.ElasticMemoryDataMsgCodec;
import edu.snu.reef.em.msg.ElasticMemoryDataMsgHandler;
import edu.snu.reef.em.driver.ElasticMemoryMessageHandlerWrapper;
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

  @Inject
  public NSWrapper(
      @Parameter(NSWrapperParameters.NameServerAddr.class) final String nameServerAddr,
      @Parameter(NSWrapperParameters.NameServerPort.class) final Integer nameServerPort,
      final ElasticMemoryDataMsgHandler elasticMemoryDataMsgHandler) throws InjectionException {

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class,
        ElasticMemoryDataMsgCodec.class)
       .bindNamedParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class,
           ExceptionHandler.class)
       .bindNamedParameter(NameServerParameters.NameServerAddr.class, nameServerAddr)
       .bindNamedParameter(NameServerParameters.NameServerPort.class, Integer.toString(nameServerPort))
       .bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, "0");
    final ElasticMemoryMessageHandlerWrapper elasticMemoryMessageHandlerWrapper =
        new ElasticMemoryMessageHandlerWrapper();
    elasticMemoryMessageHandlerWrapper.addHandler(elasticMemoryDataMsgHandler);

    final Injector networkServiceInjector = Tang.Factory.getTang().newInjector(jcb.build());
    networkServiceInjector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceHandler.class,
        elasticMemoryMessageHandlerWrapper);
    networkService = networkServiceInjector.getInstance(NetworkService.class);

  }

  public NetworkService<T> getNetworkService() {
    return networkService;
  }
}
