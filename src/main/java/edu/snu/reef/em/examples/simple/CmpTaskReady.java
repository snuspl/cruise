package edu.snu.reef.em.examples.simple;

import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;

import javax.inject.Inject;

/**
 * If heartbeat is triggered when task is ready, send a meaningless TaskMessage.
 * If not, then send nothing (Option.empty()).
 */
public final class CmpTaskReady implements TaskMessageSource {

  private Boolean ready;

  @Inject
  private CmpTaskReady() {
    this.ready = false;
  }

  @Override
  public Optional<TaskMessage> getMessage() {
    Optional<TaskMessage> retVal;

    if (ready) {
      retVal = Optional.of(TaskMessage.from(CmpTaskReady.class.getSimpleName(), new byte[0]));
      setReady(false);
    } else {
      retVal = Optional.empty();
    }

    return retVal;
  }

  public void setReady(final Boolean ready) {
    this.ready = ready;
  }
}
