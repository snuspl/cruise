/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.bsp.core;

import edu.snu.cay.dolphin.bsp.parameters.StartTrace;
import edu.snu.cay.utils.trace.HTrace;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;

/**
 * Provide tracing within classes that extend UserControllerTask/UserComputeTask.
 * To trace, turn on tracing via {@link StartTrace}.
 * The traces will be grouped by Iteration and Task.
 */
public final class UserTaskTrace {
  private TraceInfo parentTraceInfo;

  /**
   * Constructor that makes sure HTrace is injected.
   * @param hTrace
   */
  @Inject
  public UserTaskTrace(final HTrace hTrace) {
  }

  /**
   * Called by the Dolphin framework, to set the iteration and task as ancestor spans.
   * @param parentTraceInfo per-iteration TraceInfo
   */
  public void setParentTraceInfo(final TraceInfo parentTraceInfo) {
    this.parentTraceInfo = parentTraceInfo;
  }

  /**
   * Called by the UserTask, to start a new span.
   * The UserTask must call close() on the TraceScope to record the span.
   *
   * The returned span descends from iteration and task spans.
   * For more information about the TraceScope returned, see {@link Trace#startSpan(String, TraceInfo)}.
   *
   * @param description description of the span to be created
   * @return the scope of the span, which can be recorded by calling close()
   */
  public TraceScope startSpan(final String description) {
    return Trace.startSpan(description, parentTraceInfo);
  }
}
