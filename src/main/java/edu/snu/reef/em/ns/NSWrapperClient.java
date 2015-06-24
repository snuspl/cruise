package edu.snu.reef.em.ns;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.TransportFactory;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

public final class NSWrapperClient<T> {

  private final NetworkService<T> networkService;

  @Inject
  public NSWrapperClient(@Parameter(NSWrapperParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory identifierFactory,
                          @Parameter(NSWrapperParameters.NetworkServiceCodec.class) final Codec<T> codec,
                          @Parameter(NSWrapperParameters.NetworkServiceHandler.class) final EventHandler<Message<T>> recvHandler,
                          @Parameter(NSWrapperParameters.NetworkServiceExceptionHandler.class) final EventHandler<Exception> exHandler,
                          @Parameter(NSWrapperParameters.NetworkServicePort.class) final Integer networkServicePort,
                          @Parameter(NSWrapperParameters.NameServerAddr.class) final String nameServerAddr,
                          @Parameter(NSWrapperParameters.NameServerPort.class) final Integer nameServerPort,
                          @Parameter(NSWrapperParameters.NetworkServiceTransportFactory.class) final TransportFactory transportFactory) {
    this.networkService = new NetworkService<>(identifierFactory, networkServicePort, nameServerAddr, nameServerPort, codec, transportFactory, recvHandler, exHandler);
  }

  public NetworkService<T> getNetworkService() {
    return this.networkService;
  }
}
