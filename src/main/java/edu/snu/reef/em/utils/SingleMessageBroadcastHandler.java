package edu.snu.reef.em.utils;

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * A message handler helper that broadcasts a certain event to many other event
 * handlers, guaranteeing the message contains exactly one message body.
 * @param <T> message type
 */
public class SingleMessageBroadcastHandler<T> implements EventHandler<Message<T>> {
  private static final Logger LOG = Logger.getLogger(SingleMessageBroadcastHandler.class.getName());

  private final List<EventHandler<T>> handlers = new LinkedList<>();

  @Inject
  public SingleMessageBroadcastHandler() {
  }

  /**
   * Throws RuntimeException if message contains either more than one body or none.
   */
  @Override
  public final void onNext(final Message<T> msgs) {
    boolean foundMessage = false;
    T singleMsg = null;

    for (final T msg : msgs.getData()) {
      if (foundMessage) {
        throw new RuntimeException("More than one message was sent.");
      }

      foundMessage = true;
      singleMsg = msg;
    }

    if (!foundMessage) {
      throw new RuntimeException("No message contents were found.");
    }

    if (handlers.isEmpty()) {
      LOG.warning("Message arrived, but no handlers were ready to receive it.");
      return;
    }

    // start delegating messages only after
    // message has been checked that it contains only one body
    for (final EventHandler<T> handler : handlers) {
      handler.onNext(singleMsg);
    }
  }

  public final void addHandler(final EventHandler<T> handler) {
    handlers.add(handler);
  }

  public final void removeHandler(final EventHandler<T> handler) {
    handlers.remove(handler);
  }
}
