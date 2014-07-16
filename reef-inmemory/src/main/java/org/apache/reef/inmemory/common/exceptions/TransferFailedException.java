package org.apache.reef.inmemory.common.exceptions;

import java.io.IOException;

/**
 * This exception is thrown when an IOException occurred while transferring a block
 */
public class TransferFailedException extends IOException {
  public TransferFailedException(String message) {
    super(message);
  }

  public TransferFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransferFailedException(Throwable cause) {
    super(cause);
  }
}
