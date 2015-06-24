package edu.snu.reef.em.task;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import edu.snu.reef.em.avro.DataMsg;
import edu.snu.reef.em.avro.Type;
import edu.snu.reef.em.avro.UnitIdPair;
import edu.snu.reef.em.ns.api.NSWrapper;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Logger;

@TaskSide
public final class ElasticMemoryMessageSender {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMessageSender.class.getName());

  private final NetworkService<AvroElasticMemoryMessage> networkService;
  private final IdentifierFactory identifierFactory;

  @Inject
  public ElasticMemoryMessageSender(final NSWrapper<AvroElasticMemoryMessage> nsWrapper) {
    this.networkService = nsWrapper.getNetworkService();
    this.identifierFactory = new StringIdentifierFactory();
  }

  public void send(final String destId, final AvroElasticMemoryMessage msg) {
    LOG.entering(ElasticMemoryMessageSender.class.getSimpleName(), "send", new Object[] { destId, msg });
    final Connection<AvroElasticMemoryMessage> conn = networkService.newConnection(identifierFactory.getNewInstance(destId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException ex) {
      throw new RuntimeException(ex);
    }
    LOG.exiting(ElasticMemoryMessageSender.class.getSimpleName(), "send", new Object[] { destId, msg });
  }

  public void send(final String destId, final String dataClassName, final List<UnitIdPair> unitIdPairList) {
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
  }
}
