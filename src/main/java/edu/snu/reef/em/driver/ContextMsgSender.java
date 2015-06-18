package edu.snu.reef.em.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.parameters.ContextActiveHandlers;
import org.apache.reef.driver.parameters.TaskRunningHandlers;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

@Unit
@DriverSide
public final class ContextMsgSender {
  private Map<String, ActiveContext> contextMap;

  @Inject
  ContextMsgSender() {
    contextMap = new HashMap<>();
  }

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
