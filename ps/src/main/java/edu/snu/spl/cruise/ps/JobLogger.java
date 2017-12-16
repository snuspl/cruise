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
package edu.snu.spl.cruise.ps;

import org.apache.reef.tang.annotations.Parameter;
import sun.misc.JavaLangAccess;
import sun.misc.SharedSecrets;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A logger for cruise jobs, which distinguishes jobs with the injected
 * {@link edu.snu.spl.cruise.ps.CruiseParameters.CruiseJobId}.
 * This class relays log messages to the {@link Logger}, appending job id to log messages.
 */
public final class JobLogger {
  private final String msgPrefix;
  private final Logger logger;

  @Inject
  private JobLogger(@Parameter(CruiseParameters.CruiseJobId.class) final String cruiseJobId) throws IOException {
    this.msgPrefix = "[JobId: " + cruiseJobId + "] ";
    this.logger = Logger.getLogger(JobLogger.class.getName());
  }

  /**
   * Overrides {@link Logger#log(Level, String)}.
   */
  public void log(final Level level, final String msg) {
    final LogRecord lr = new LogRecord(level, msgPrefix + msg);
    inferCaller(lr);

    logger.log(lr);
  }

  /**
   * Overrides {@link Logger#log(Level, String, Object)}.
   */
  public void log(final Level level, final String msg, final Object param1) {
    final LogRecord lr = new LogRecord(level, msgPrefix + msg);
    lr.setParameters(new Object[]{param1});
    inferCaller(lr);

    logger.log(lr);
  }

  /**
   * Overrides {@link Logger#log(Level, String, Object[])}.
   */
  public void log(final Level level, final String msg, final Object[] params) {
    final LogRecord lr = new LogRecord(level, msgPrefix + msg);
    lr.setParameters(params);
    inferCaller(lr);

    logger.log(lr);
  }

  /**
   * Infer caller's class and method name and set them to the given {@link LogRecord}.
   * Borrowed from {@link LogRecord}.
   */
  private void inferCaller(final LogRecord lr) {
    final JavaLangAccess access = SharedSecrets.getJavaLangAccess();
    final Throwable throwable = new Throwable();
    final int depth = access.getStackTraceDepth(throwable);

    boolean lookingForLogger = true;
    for (int ix = 0; ix < depth; ix++) {
      // Calling getStackTraceElement directly prevents the VM
      // from paying the cost of building the entire stack frame.
      final StackTraceElement frame =
          access.getStackTraceElement(throwable, ix);
      final String cname = frame.getClassName();
      final boolean isLoggerImpl = isLoggerImplFrame(cname);
      if (lookingForLogger) {
        // Skip all frames until we have found the first logger frame.
        if (isLoggerImpl) {
          lookingForLogger = false;
        }
      } else {
        if (!isLoggerImpl) {
          // skip reflection call
          if (!cname.startsWith("java.lang.reflect.") && !cname.startsWith("sun.reflect.")) {
            // We've found the relevant frame.
            lr.setSourceClassName(cname);
            lr.setSourceMethodName(frame.getMethodName());
            return;
          }
        }
      }
    }
  }

  /**
   * Borrowed from {@link LogRecord}.
   */
  private boolean isLoggerImplFrame(final String cname) {
    // the log record could be created for a platform logger
    return (cname.equals("java.util.logging.Logger") ||
        cname.startsWith("java.util.logging.LoggingProxyImpl") ||
        cname.startsWith("sun.util.logging.") ||
        cname.startsWith(JobLogger.class.getName())); // modified line
  }
}
