package edu.snu.cay.services.em.utils;

import org.apache.reef.io.network.Message;

public final class SingleMessageExtractor {

  /**
   * Should not be instantiated
   */
  private SingleMessageExtractor() {
  }

  public static <T> T extract(final Message<T> msg) {
    boolean foundMessage = false;
    T singleMsg = null;

    for (final T innerMsg : msg.getData()) {
      if (foundMessage) {
        throw new RuntimeException("More than one message was sent.");
      }

      foundMessage = true;
      singleMsg = innerMsg;
    }

    if (!foundMessage) {
      throw new RuntimeException("No message contents were found.");
    }

    return singleMsg;
  }
}
