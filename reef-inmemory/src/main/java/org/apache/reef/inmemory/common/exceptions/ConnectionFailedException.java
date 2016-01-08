package org.apache.reef.inmemory.common.exceptions;

import java.io.IOException;

/**
 * This exception is thrown when failed while making a connection.
 */
public class ConnectionFailedException extends IOException {
  public ConnectionFailedException() {
  }

  public ConnectionFailedException(String message) {
    super(message);
  }

  public ConnectionFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionFailedException(Throwable cause) {
    super(cause);
  }
}
