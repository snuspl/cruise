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
package edu.snu.cay.dolphin.async.jobserver;

import org.apache.reef.runtime.common.driver.idle.DriverIdleManager;
import org.apache.reef.runtime.common.driver.idle.DriverIdlenessSource;
import org.apache.reef.runtime.common.driver.idle.IdleMessage;
import org.apache.reef.tang.InjectionFuture;
import edu.snu.cay.dolphin.async.jobserver.JobServerDriver.ClientMessageHandler;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * It determines a termination of job server.
 * If {@link ClientMessageHandler} receives command to shut down the job server.
 * The server calls {@link #finishJobServer()}.
 */
public final class JobServerStatusManager implements DriverIdlenessSource {

  private static final Logger LOG = Logger.getLogger(JobServerStatusManager.class.getName());
  private static final IdleMessage RUNNING_MSG =
      new IdleMessage("JobServerStatusManager", "JobServer is still running", false);
  private static final IdleMessage FINISH_MSG =
      new IdleMessage("JobServerStatusManager", "JobServer finished", true);

  private final InjectionFuture<DriverIdleManager> driverIdleManagerFuture;
  private volatile boolean isJobServerRunning;

  @Inject
  private JobServerStatusManager(final InjectionFuture<DriverIdleManager> driverIdleManagerFuture) {
    this.driverIdleManagerFuture = driverIdleManagerFuture;
    this.isJobServerRunning = true;
  }

  @Override
  public IdleMessage getIdleStatus() {
    return isJobServerRunning ? RUNNING_MSG : FINISH_MSG;
  }

  /**
   * Sends {@link IdleMessage} to {@link DriverIdleManager} to terminate a job.
   */
  void finishJobServer() {
    isJobServerRunning = false;
    driverIdleManagerFuture.get().onPotentiallyIdle(FINISH_MSG);
  }
}
