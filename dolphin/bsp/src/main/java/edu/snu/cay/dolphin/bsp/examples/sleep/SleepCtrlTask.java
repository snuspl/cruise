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
package edu.snu.cay.dolphin.bsp.examples.sleep;

import edu.snu.cay.common.param.Parameters.MaxNumEpochs;
import edu.snu.cay.dolphin.bsp.core.UserControllerTask;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataReduceReceiver;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The {@link UserControllerTask} for SleepREEF.
 * Does not do any significant computation; simply prints log message for each iteration.
 */
public final class SleepCtrlTask extends UserControllerTask
    implements DataReduceReceiver<Object>, DataBroadcastSender<Object> {
  private static final Logger LOG = Logger.getLogger(SleepCtrlTask.class.getName());

  private final int maxIterations;
  private final Object broadcastObject;

  @Inject
  private SleepCtrlTask(@Parameter(MaxNumEpochs.class) final int maxIterations) {
    this.maxIterations = maxIterations;
    this.broadcastObject = new Object();
  }

  @Override
  public void run(final int iteration) {
    LOG.log(Level.INFO, "Run iteration {0}", iteration);
  }

  @Override
  public boolean isTerminated(final int iteration) {
    return iteration >= maxIterations;
  }

  @Override
  public void receiveReduceData(final int iteration, final Object data) {
    LOG.log(Level.FINE, "Received {0} for iteration {1}", new Object[]{data, iteration});
  }

  @Override
  public Object sendBroadcastData(final int iteration) {
    LOG.log(Level.FINE, "Sending {0} for iteration {1}", new Object[]{broadcastObject, iteration});
    return broadcastObject;
  }
}
