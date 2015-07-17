package edu.snu.cay.services.em.ns.impl;

import edu.snu.cay.services.em.ns.NSWrapperParameters;
import edu.snu.cay.services.em.ns.api.NSWrapper;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.TransportFactory;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;

/**
 * Creates a single NetworkService instance that doesn't use Tang.
 * This class is the ad-hoc solution of creating more than one instance of the
 * same class while using Tang.
 */
public final class NSWrapperImpl<T> implements NSWrapper<T> {

  private final NetworkService<T> networkService;

  /**
   * Constructor used when all NSWrapperParameters are provided.
   * Usually, this constructor gets called at evaluators.
   */
  @Inject
  private NSWrapperImpl(@Parameter(NSWrapperParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory identifierFactory,
                        @Parameter(NSWrapperParameters.NetworkServiceCodec.class) final Codec<T> codec,
                        @Parameter(NSWrapperParameters.NetworkServiceHandler.class) final EventHandler<Message<T>> recvHandler,
                        @Parameter(NSWrapperParameters.NetworkServiceExceptionHandler.class) final EventHandler<Exception> exHandler,
                        @Parameter(NSWrapperParameters.NetworkServicePort.class) final Integer networkServicePort,
                        @Parameter(NSWrapperParameters.NameServerAddr.class) final String nameServerAddr,
                        @Parameter(NSWrapperParameters.NameServerPort.class) final Integer nameServerPort,
                        @Parameter(NSWrapperParameters.NetworkServiceTransportFactory.class) final TransportFactory transportFactory) {
    this.networkService = new NetworkService<>(identifierFactory, networkServicePort, nameServerAddr, nameServerPort, codec, transportFactory, recvHandler, exHandler);
  }

  /**
   * Constructor used when NameServerAddr and NameServerPort are not set.
   * Instead of explicitly setting parameters, this constructor receives a
   * NameServer injection by Tang and uses the NameServer instance to figure
   * out NameServerAddr and NameServerPort.
   * Usually, this constructor gets called at the driver.
   */
  @Inject
  private NSWrapperImpl(@Parameter(NSWrapperParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory identifierFactory,
                        @Parameter(NSWrapperParameters.NetworkServiceCodec.class) final Codec<T> codec,
                        @Parameter(NSWrapperParameters.NetworkServiceHandler.class) final EventHandler<Message<T>> recvHandler,
                        @Parameter(NSWrapperParameters.NetworkServiceExceptionHandler.class) final EventHandler<Exception> exHandler,
                        @Parameter(NSWrapperParameters.NetworkServicePort.class) final Integer networkServicePort,
                        final NameServer nameServer,
                        final LocalAddressProvider localAddressProvider,
                        @Parameter(NSWrapperParameters.NetworkServiceTransportFactory.class) final TransportFactory transportFactory) {
    final String nameServerAddr = localAddressProvider.getLocalAddress();
    final int nameServerPort = nameServer.getPort();
    this.networkService = new NetworkService<>(identifierFactory, networkServicePort, nameServerAddr, nameServerPort, codec, transportFactory, recvHandler, exHandler);
  }

  @Override
  public NetworkService<T> getNetworkService() {
    return this.networkService;
  }
}
