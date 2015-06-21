package edu.snu.reef.em.driver;

import edu.snu.reef.em.msg.ElasticMemoryCtrlMsg;
import edu.snu.reef.em.msg.ElasticMemoryCtrlMsgCodec;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

@DriverSide
public class ElasticMemoryServiceImpl implements ElasticMemoryService {

  private final ContextMsgSender contextMsgSender;
  private final ElasticMemoryCtrlMsgCodec msgCodec;
  private final EvaluatorRequestor requestor;

  @Inject
  private ElasticMemoryServiceImpl(final ContextMsgSender contextMsgSender,
                                   final ElasticMemoryCtrlMsgCodec msgCodec,
                                   final EvaluatorRequestor requestor) {
    this.contextMsgSender = contextMsgSender;
    this.msgCodec = msgCodec;
    this.requestor = requestor;
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
    contextMsgSender.send(srcEvalId, msgCodec.encode(new ElasticMemoryCtrlMsg(dataClassName, destEvalId)));
  }

  @Override
  public void checkpoint(String evalId) {
    throw new NotImplementedException();
  }
}
