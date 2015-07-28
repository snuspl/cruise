package edu.snu.cay.services.em.ns;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.ns.parameters.EMCodec;
import edu.snu.cay.services.em.ns.parameters.EMIdentifier;
import edu.snu.cay.services.em.ns.parameters.EMMessageHandler;
import edu.snu.cay.services.em.ns.parameters.NameServerAddr;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

public final class EMNetworkSetup {
  private final ConnectionFactory connectionFactory;

  @Inject
  private EMNetworkSetup(
      final NetworkConnectionService networkConnectionService,
      final IdentifierFactory identifierFactory,
      @Parameter(EMIdentifier.class) final String idString,
      @Parameter(EMCodec.class) final Codec<AvroElasticMemoryMessage> codec,
      @Parameter(EMMessageHandler.class) final EventHandler<Message<AvroElasticMemoryMessage>> handler
  ) throws NetworkException {

    final Identifier identifier = identifierFactory.getNewInstance(idString);
    networkConnectionService.registerConnectionFactory(identifier, codec, handler, null);
    this.connectionFactory = networkConnectionService.getConnectionFactory(identifier);
  }

  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }
}
