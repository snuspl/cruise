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
package edu.snu.cay.dolphin.core;

import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataGatherSender;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataScatterReceiver;

/**
 * Abstract class for user-defined compute tasks.
 * This class should be extended by user-defined compute tasks that override run, initialize, and cleanup methods
 */
public abstract class UserComputeTask {

  /**
   * Main process of a user-defined compute task
   * @param iteration
   */
  public abstract void run(int iteration);

  /**
   * Initialize a user-defined compute task.
   * Default behavior of this method is to do nothing, but this method can be overridden in subclasses
   */
  public void initialize() throws ParseException {
    return;
  }

  /**
   * Clean up a user-defined compute task
   * Default behavior of this method is to do nothing, but this method can be overridden in subclasses
   */
  public void cleanup() {
    return;
  }

  final public boolean isReduceUsed() {
    return (this instanceof DataReduceSender);
  }

  final public boolean isGatherUsed() {
    return (this instanceof DataGatherSender);
  }

  final public boolean isBroadcastUsed() {
    return (this instanceof DataBroadcastReceiver);
  }

  final public boolean isScatterUsed() {
    return (this instanceof DataScatterReceiver);
  }
}
