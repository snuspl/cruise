package edu.snu.reef.em.examples.elastic.migration;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;

import javax.inject.Inject;

public final class CmpTaskReady implements TaskMessageSource {

  private final Codec<Boolean> codec;
  private Boolean ready;

  @Inject
  public CmpTaskReady(final ReadyCodec codec) {
    this.codec = codec;
    this.ready = false;
  }

  @Override
  public Optional<TaskMessage> getMessage() {
    Optional<TaskMessage> retVal;

    if (ready) {
      retVal = Optional.of(TaskMessage.from(CmpTaskReady.class.getSimpleName(),
          codec.encode(true)));
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
