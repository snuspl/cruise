package edu.snu.reef.em.task;

import edu.snu.reef.em.msg.ElasticMemoryDataMsg;
import org.apache.reef.annotations.audience.TaskSide;
import edu.snu.reef.em.ns.NSWrapper;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.logging.Logger;

@TaskSide
public final class ElasticMemoryMessageSender {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMessageSender.class.getName());

  private final NetworkService<ElasticMemoryDataMsg> networkService;
  private final IdentifierFactory identifierFactory;

  @Inject
  public ElasticMemoryMessageSender(final NSWrapper<ElasticMemoryDataMsg> nsWrapper) {
    this.networkService = nsWrapper.getNetworkService();
    this.identifierFactory = new StringIdentifierFactory();
  }

  public void send(final String destId, final ElasticMemoryDataMsg msg) {
    LOG.entering(ElasticMemoryMessageSender.class.getSimpleName(), "send", new Object[] { destId, msg });
    final Connection<ElasticMemoryDataMsg> conn = networkService.newConnection(identifierFactory.getNewInstance(destId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException ex) {
      throw new RuntimeException(ex);
    }
    LOG.exiting(ElasticMemoryMessageSender.class.getSimpleName(), "send", new Object[] { destId, msg });
  }

  public void send(final String destId, final String dataClassName, final byte[][] data) {
    send(destId,
        new ElasticMemoryDataMsg(networkService.getMyId().toString(), destId, dataClassName, data));
  }
}
