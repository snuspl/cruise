package edu.snu.reef.em.driver.impl;

import edu.snu.reef.em.driver.api.ElasticMemory;
import edu.snu.reef.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.reef.em.ns.api.NSWrapper;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Set;

@DriverSide
public final class ElasticMemoryImpl implements ElasticMemory {
  private final EvaluatorRequestor requestor;
  private final ElasticMemoryMsgSender sender;

  @Inject
  private ElasticMemoryImpl(final EvaluatorRequestor requestor,
                            @Parameter(DriverIdentifier.class) final String driverId,
                            final ElasticMemoryMsgSender sender,
                            final NSWrapper nsWrapper) {
    this.requestor = requestor;
    this.sender = sender;
    nsWrapper.getNetworkService().registerId(nsWrapper.getNetworkService().getIdentifierFactory().getNewInstance(driverId));
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
    sender.sendCtrlMsg(srcEvalId, dataClassName, destEvalId);
  }

  // TODO: implement
  @Override
  public void checkpoint(final String evalId) {
    throw new NotImplementedException();
  }
}
