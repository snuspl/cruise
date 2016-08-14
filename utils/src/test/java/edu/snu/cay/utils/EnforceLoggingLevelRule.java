/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * TestWatcher implementation to override the logging level of specific logger to the specified level.
 * The overridden level lasts during a specific jUnit test method and
 * is restored after the test method is finished.
 */
public final class EnforceLoggingLevelRule extends TestWatcher {

  private final String methodName;

  private final Logger logger;

  private final Level origLoggingLevel;

  private final Level targetLoggingLevel;

  /**
   * Constructor of {@link EnforceLoggingLevelRule}.
   * This rule overrides the logging level of logger {@code loggerName} with {@code targetLoggingLevel}
   * in the scope of a jUnit test named {@code methodName}.
   * @param methodName the method name of jUnit test
   * @param loggerName the name of logger (Normally we use the class name)
   * @param targetLoggingLevel a logging level to override the logger with
   */
  public EnforceLoggingLevelRule(final String methodName,
                                 final String loggerName,
                                 final Level targetLoggingLevel) {
    this.methodName = methodName;
    this.logger = Logger.getLogger(loggerName);
    this.origLoggingLevel = logger.getLevel();
    this.targetLoggingLevel = targetLoggingLevel;
  }

  @Override
  public void starting(final Description desc) {
    if (desc.getMethodName().equals(methodName)) {
      logger.setLevel(targetLoggingLevel);
    }
  }

  @Override
  public void finished(final Description desc) {
    if (desc.getMethodName().equals(methodName)) {
      logger.setLevel(origLoggingLevel);
    }
  }
}
