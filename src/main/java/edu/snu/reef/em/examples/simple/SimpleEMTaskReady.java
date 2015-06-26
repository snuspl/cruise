package edu.snu.reef.em.examples.simple;

import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;

import javax.inject.Inject;

/**
 * If heartbeat is triggered when task is ready, send a meaningless TaskMessage.
 * If not, then send nothing (Option.empty()).
 */
final class SimpleEMTaskReady implements TaskMessageSource {

  private boolean ready;

  @Inject
  private SimpleEMTaskReady() {
    this.ready = false;
  }

  @Override
  public Optional<TaskMessage> getMessage() {
    final Optional<TaskMessage> retVal;

    if (ready) {
      retVal = Optional.of(TaskMessage.from(SimpleEMTaskReady.class.getSimpleName(), new byte[0]));
      setReady(false);
    } else {
      retVal = Optional.empty();
    }

    return retVal;
  }

  public void setReady(final boolean ready) {
    this.ready = ready;
  }
}
