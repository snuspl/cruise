package edu.snu.reef.em.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.parameters.ContextActiveHandlers;
import org.apache.reef.driver.parameters.TaskRunningHandlers;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for sending messages to evaluator Contexts using the Driver's remote manager
 */
@Unit
@DriverSide
public final class ContextMsgSender {

  /**
   * Map of Context/Task ids to the corresponding ActiveContext object
   */
  private Map<String, ActiveContext> contextMap;

  @Inject
  private ContextMsgSender() {
    contextMap = new HashMap<>();
  }

  /**
   * Send a byte array message to the specified Context.
   * If the string id indicates a Task, then the message is sent to the Context
   * that is directly beneath that Task
   *
   * @param id Context/Task string identifier
   * @param msg byte array message to send
   */
  public void send(final String id, final byte[] msg) {
    final ActiveContext activeContext = contextMap.get(id);
    if (activeContext == null) {
      throw new RuntimeException("No such activeContext or runningTask with the id " + id);
    }

    activeContext.sendMessage(msg);
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext activeContext) {
      contextMap.put(activeContext.getId(), activeContext);
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(RunningTask runningTask) {
      contextMap.put(runningTask.getId(), runningTask.getActiveContext());
    }
  }

  public static Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextActiveHandlers.class, ActiveContextHandler.class)
        .bindSetEntry(TaskRunningHandlers.class, RunningTaskHandler.class)
        .build();
  }

  @Override
  public String toString() {
    return ContextMsgSender.class.getSimpleName() + ": " + contextMap.toString();
  }
}
