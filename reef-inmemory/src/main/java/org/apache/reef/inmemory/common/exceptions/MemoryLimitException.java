package org.apache.reef.inmemory.common.exceptions;

import java.io.IOException;

/**
 * This exception is thrown when failed when the memory limit has been reached
 */
public class MemoryLimitException extends IOException {
  public MemoryLimitException() {
  }

  public MemoryLimitException(String message) {
    super(message);
  }

  public MemoryLimitException(String message, Throwable cause) {
    super(message, cause);
  }

  public MemoryLimitException(Throwable cause) {
    super(cause);
  }
}
