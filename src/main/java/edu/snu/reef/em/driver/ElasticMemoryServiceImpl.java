package edu.snu.reef.em.driver;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import edu.snu.reef.em.avro.CtrlMsg;
import edu.snu.reef.em.avro.Type;
import edu.snu.reef.em.msg.ElasticMemoryMessageCodec;
import edu.snu.reef.em.ns.NSWrapperClient;
import edu.snu.reef.em.task.ElasticMemoryMessageSender;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.io.network.group.impl.driver.ExceptionHandler;
import org.apache.reef.io.network.impl.MessagingTransportFactory;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.util.Set;

@DriverSide
public final class ElasticMemoryServiceImpl implements ElasticMemoryService {
  private final EvaluatorRequestor requestor;
  private final ElasticMemoryMessageSender sender;

  @Inject
  private ElasticMemoryServiceImpl(final EvaluatorRequestor requestor,
                                   final NameServer nameServer,
                                   final LocalAddressProvider localAddressProvider,
                                   @Parameter(DriverIdentifier.class) final String driverId) {
    this.requestor = requestor;

    final NSWrapperClient<AvroElasticMemoryMessage> nsWrapper =
        new NSWrapperClient<>(new StringIdentifierFactory(),
            new ElasticMemoryMessageCodec(),
            new ElasticMemoryMessageHandlerWrapperImpl(),
            new ExceptionHandler(),
            0,
            localAddressProvider.getLocalAddress(),
            nameServer.getPort(),
            new MessagingTransportFactory());
    nsWrapper.getNetworkService().registerId(nsWrapper.getNetworkService().getIdentifierFactory().getNewInstance(driverId));

    this.sender = new ElasticMemoryMessageSender(nsWrapper);
  }

  @Override
  public void add(final int number, final int megaBytes, final int cores) {
    requestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(number)
        .setMemory(megaBytes)
        .setNumberOfCores(cores)
        .build());
  }

  @Override
  public void delete(final String evalId) {
    throw new NotImplementedException();
  }

  @Override
  public void resize(final String evalId, final int megaBytes, final int cores) {
    throw new NotImplementedException();
  }

  @Override
  public void move(String dataClassName, Set<IntRange> rangeSet, String srcEvalId, String destEvalId) {

    final AvroElasticMemoryMessage msg = AvroElasticMemoryMessage.newBuilder()
        .setType(Type.CtrlMsg)
        .setSrcId(srcEvalId)
        .setDestId(destEvalId)
        .setCtrlMsg(CtrlMsg.newBuilder().setDataClassName(dataClassName).build())
        .build();

    sender.send(srcEvalId, msg);
  }

  @Override
  public void checkpoint(String evalId) {
    throw new NotImplementedException();
  }
}
