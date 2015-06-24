package edu.snu.reef.em.msg.impl;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import edu.snu.reef.em.avro.DataMsg;
import edu.snu.reef.em.avro.Type;
import edu.snu.reef.em.avro.UnitIdPair;
import edu.snu.reef.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.reef.em.ns.api.NSWrapper;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Logger;


/**
 * Sender class that uses a NetworkService instance provided by NSWrapper to
 * send AvroElasticMemoryMessages to the driver and evaluators.
 */
public final class ElasticMemoryMsgSenderImpl implements ElasticMemoryMsgSender {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgSenderImpl.class.getName());

  private final NetworkService<AvroElasticMemoryMessage> networkService;
  private final IdentifierFactory identifierFactory;

  // TODO: declared `public` because of ElasticMemoryImpl.
  // Should be instantiated through Tang.
  @Inject
  public ElasticMemoryMsgSenderImpl(final NSWrapper<AvroElasticMemoryMessage> nsWrapper) {
    this.networkService = nsWrapper.getNetworkService();
    this.identifierFactory = this.networkService.getIdentifierFactory();
  }

  public void send(final String destId, final AvroElasticMemoryMessage msg) {
    LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "send", new Object[] { destId, msg });

    final Connection<AvroElasticMemoryMessage> conn = networkService.newConnection(identifierFactory.getNewInstance(destId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException ex) {
      throw new RuntimeException("NetworkException", ex);
    }

    LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "send", new Object[] { destId, msg });
  }

  public void sendDataMsg(final String destId, final String dataClassName, final List<UnitIdPair> unitIdPairList) {
    LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataMsg", new Object[] { destId, dataClassName });

    final DataMsg dataMsg = DataMsg.newBuilder()
        .setDataClassName(dataClassName)
        .setUnits(unitIdPairList)
        .build();

    send(destId,
         AvroElasticMemoryMessage.newBuilder()
                                 .setType(Type.DataMsg)
                                 .setSrcId(networkService.getMyId().toString())
                                 .setDestId(destId)
                                 .setDataMsg(dataMsg)
                                 .build());

    LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataMsg", new Object[] { destId, dataClassName });
  }
}
