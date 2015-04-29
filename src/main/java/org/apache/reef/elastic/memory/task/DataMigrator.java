package org.apache.reef.elastic.memory.task;

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.elastic.memory.utils.NSWrapper;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

@TaskSide
final class DataMigrator {

  private int receiveCount;

  class ReceiveHandler implements EventHandler<Message<String>> {
    @Override
    public void onNext(Message<String> msg) {
      System.out.println("Received from NetworkServiceWrapper");
      System.out.println(msg.getData().toString() + receiveCount);
      receiveCount++;
    }
  }

  private final NetworkService networkService;
  private final IdentifierFactory identifierFactory;

  @Inject
  public DataMigrator(
      final NSWrapper nsWrapper) {
    this.networkService = nsWrapper.getNetworkService();
    this.identifierFactory = new StringIdentifierFactory();
    nsWrapper.getNetworkServiceHandler().registerReceiverHandler(new ReceiveHandler());
    this.receiveCount = 0;
  }

  public void move(String destTaskId, String data){
    Connection<String> conn = networkService.newConnection(identifierFactory.getNewInstance(destTaskId));

    try {
      conn.open();
      conn.write(data);
      conn.close();
    } catch (NetworkException ex) {
      String msg = "error while move data to other evaluator";
      throw new RuntimeException(msg, ex);
    }
  }
}
