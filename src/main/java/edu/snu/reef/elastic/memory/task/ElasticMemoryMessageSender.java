package edu.snu.reef.elastic.memory.task;

import edu.snu.reef.elastic.memory.ElasticMemoryMessage;
import org.apache.reef.annotations.audience.TaskSide;
import edu.snu.reef.elastic.memory.ns.NSWrapper;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@TaskSide
public final class ElasticMemoryMessageSender {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMessageSender.class.getName());

  private final NetworkService<ElasticMemoryMessage> networkService;
  private final IdentifierFactory identifierFactory;
  private final String selfId;

  @Inject
  public ElasticMemoryMessageSender(final NSWrapper<ElasticMemoryMessage> nsWrapper,
                                    @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId) {
    this.networkService = nsWrapper.getNetworkService();
    this.identifierFactory = new StringIdentifierFactory();
    this.selfId = selfId;
  }

  public void send(final String destId, final ElasticMemoryMessage msg) {
    LOG.entering(ElasticMemoryMessageSender.class.getSimpleName(), "send", new Object[] { destId, msg });
    final Connection<ElasticMemoryMessage> conn = networkService.newConnection(identifierFactory.getNewInstance(destId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException ex) {
      throw new RuntimeException(ex);
    }
    LOG.exiting(ElasticMemoryMessageSender.class.getSimpleName(), "send", new Object[] { destId, msg });
  }

  public void send(final String destId, final String dataClassName, final byte[] data) {
    send(destId,
        new ElasticMemoryMessage(selfId, destId, ElasticMemoryMessage.Type.DATA, dataClassName, data));
  }
}
