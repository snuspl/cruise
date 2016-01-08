package org.apache.reef.inmemory.task.hdfs;

import java.io.IOException;

/**
 * This exception is thrown when when it fails to decode the Token.
 */
public class TokenDecodeFailedException extends IOException {
  public TokenDecodeFailedException(String message) {
    super(message);
  }

  public TokenDecodeFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public TokenDecodeFailedException(Throwable cause) {
    super(cause);
  }
}
