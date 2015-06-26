package edu.snu.reef.em.driver.impl;

import edu.snu.reef.em.avro.AvroElasticMemoryMessage;
import edu.snu.reef.em.avro.CtrlMsg;
import edu.snu.reef.em.avro.Type;
import edu.snu.reef.em.driver.ElasticMemoryMsgHandlerDriver;
import edu.snu.reef.em.driver.api.ElasticMemory;
import edu.snu.reef.em.msg.ElasticMemoryMsgCodec;
import edu.snu.reef.em.msg.ElasticMemoryMsgBroadcaster;
import edu.snu.reef.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.reef.em.ns.NSWrapperParameters;
import edu.snu.reef.em.ns.impl.NSWrapperImpl;
import edu.snu.reef.em.ns.api.NSWrapper;
import edu.snu.reef.em.msg.impl.ElasticMemoryMsgSenderImpl;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.io.network.TransportFactory;
import org.apache.reef.io.network.group.impl.driver.ExceptionHandler;
import org.apache.reef.io.network.impl.MessagingTransportFactory;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.util.Set;

@DriverSide
public final class ElasticMemoryImpl implements ElasticMemory {
  private final EvaluatorRequestor requestor;
  private final ElasticMemoryMsgSender sender;

  @Inject
  private ElasticMemoryImpl(final EvaluatorRequestor requestor,
                            final LocalAddressProvider localAddressProvider,
                            final NameServer nameServer,
                            @Parameter(DriverIdentifier.class) final String driverId,
                            final ElasticMemoryMsgCodec elasticMemoryMsgCodec,
                            @Parameter(NSWrapperParameters.NetworkServiceTransportFactory.class) final TransportFactory transportFactory,
                            final ExceptionHandler exceptionHandler,
                            @Parameter(NSWrapperParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory ifac,
                            final ElasticMemoryMsgBroadcaster broadcaster,
                            final ElasticMemoryMsgHandlerDriver handlerDriver) {
    this.requestor = requestor;
    broadcaster.addHandler(handlerDriver);

    // TODO: To receive a Tang injection of NSWrapper, Tang must know the
    // NameServer's address and port beforehand. However, the client may not
    // provide Tang with the information, and thus we currently use
    // `new` to instantiate NSWrapper.
    final NSWrapper<AvroElasticMemoryMessage> nsWrapper =
        new NSWrapperImpl<>(ifac,
                            elasticMemoryMsgCodec,
                            broadcaster,
                            exceptionHandler,
                            0,
                            localAddressProvider.getLocalAddress(),
                            nameServer.getPort(),
                            transportFactory);
    nsWrapper.getNetworkService().registerId(nsWrapper.getNetworkService().getIdentifierFactory().getNewInstance(driverId));

    this.sender = new ElasticMemoryMsgSenderImpl(nsWrapper);
  }

  @Override
  public void add(final int number, final int megaBytes, final int cores) {
    requestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(number)
        .setMemory(megaBytes)
        .setNumberOfCores(cores)
        .build());
  }

  // TODO: implement
  @Override
  public void delete(final String evalId) {
    throw new NotImplementedException();
  }

  // TODO: implement
  @Override
  public void resize(final String evalId, final int megaBytes, final int cores) {
    throw new NotImplementedException();
  }

  // TODO: @param rangeSet is currently not being used.
  @Override
  public void move(final String dataClassName, final Set<IntRange> rangeSet, final String srcEvalId, final String destEvalId) {
    final AvroElasticMemoryMessage msg = AvroElasticMemoryMessage.newBuilder()
        .setType(Type.CtrlMsg)
        .setSrcId(srcEvalId)
        .setDestId(destEvalId)
        .setCtrlMsg(CtrlMsg.newBuilder().setDataClassName(dataClassName).build())
        .build();

    sender.send(srcEvalId, msg);
  }

  // TODO: implement
  @Override
  public void checkpoint(final String evalId) {
    throw new NotImplementedException();
  }
}
