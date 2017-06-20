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

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * It determines a termination of job server.
 * If {@link JobServerHttpHandler} receives HTTP request to shut down the job server.
 * The server calls {@link #finishJobServer()}.
 */
public final class JobServerTerminator implements DriverIdlenessSource {

  private static final Logger LOG = Logger.getLogger(JobServerTerminator.class.getName());
  private static final IdleMessage JOB_RUNNING_MSG =
      new IdleMessage("JobServerTerminator", "Job is still running", false);
  private static final IdleMessage JOB_FINISH_MSG =
      new IdleMessage("JobServerTerminator", "Job finished", true);

  private final InjectionFuture<DriverIdleManager> driverIdleManagerFuture;
  private boolean isJobRunning;

  @Inject
  private JobServerTerminator(final InjectionFuture<DriverIdleManager> driverIdleManagerFuture) {
    this.driverIdleManagerFuture = driverIdleManagerFuture;
    this.isJobRunning = true;
  }

  @Override
  public IdleMessage getIdleStatus() {
    return isJobRunning ? JOB_RUNNING_MSG : JOB_FINISH_MSG;
  }

  /**
   * Sends {@link IdleMessage} to {@link DriverIdleManager} to terminate a job.
   */
  public void finishJobServer() {
    isJobRunning = false;
    driverIdleManagerFuture.get().onPotentiallyIdle(JOB_FINISH_MSG);
  }

}
