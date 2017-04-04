/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.et.exceptions;

import java.util.concurrent.ExecutionException;

/**
 * An exception thrown when access to table has failed.
 */
public class DataAccessFailedException extends ExecutionException {

  public DataAccessFailedException(final String msg) {
    super(msg);
  }

  public DataAccessFailedException(final Throwable cause) {
    super(cause);
  }

  public DataAccessFailedException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
}
