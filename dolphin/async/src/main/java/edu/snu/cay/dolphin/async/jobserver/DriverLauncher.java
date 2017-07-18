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

import edu.snu.cay.dolphin.async.JobMessageLogger;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Extended version of {@link org.apache.reef.client.DriverLauncher} for job server.
 * It has a {@link CommandListener} that receives job command messages from {@link CommandSender}
 * and redirects the messages to {@link JobServerDriver} using {@link RunningJob#send(byte[])}.
 */
@ClientSide
@Unit
public final class DriverLauncher implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(DriverLauncher.class.getName());

  private static final Configuration CLIENT_CONFIG = ClientConfiguration.CONF
      .set(ClientConfiguration.ON_JOB_SUBMITTED, SubmittedJobHandler.class)
      .set(ClientConfiguration.ON_JOB_RUNNING, RunningJobHandler.class)
      .set(ClientConfiguration.ON_JOB_COMPLETED, CompletedJobHandler.class)
      .set(ClientConfiguration.ON_JOB_FAILED, FailedJobHandler.class)
      .set(ClientConfiguration.ON_RUNTIME_ERROR, RuntimeErrorHandler.class)
      .set(ClientConfiguration.ON_JOB_MESSAGE, JobMessageLogger.class)
      .build();

  private final REEF reef;

  private volatile LauncherStatus status = LauncherStatus.INIT;

  private volatile String jobId;
  private volatile RunningJob theJob;
  private final CommandListener commandListener;

  @Inject
  private DriverLauncher(final REEF reef,
                         final CommandListener commandListener) {
    this.reef = reef;
    this.commandListener = commandListener;
  }

  /**
   * Instantiate a launcher for the given Configuration.
   *
   * @param runtimeConfiguration the resourcemanager configuration to be used
   * @return a DriverLauncher based on the given resourcemanager configuration
   * @throws InjectionException on configuration errors
   */
  public static DriverLauncher getLauncher(final Configuration runtimeConfiguration) throws InjectionException {
    return Tang.Factory.getTang()
        .newInjector(runtimeConfiguration, CLIENT_CONFIG)
        .getInstance(DriverLauncher.class);
  }

  /**
   * Kills the running job.
   */
  @Override
  public void close() throws Exception {
    synchronized (this) {
      LOG.log(Level.FINER, "Close launcher: job {0} with status {1}", new Object[] {theJob, status});
      if (this.status.isRunning()) {
        status = LauncherStatus.FORCE_CLOSED;
      }
      if (null != theJob) {
        theJob.close();
      }
      notify();
    }
    LOG.log(Level.FINEST, "Close launcher: shutdown REEF");
    commandListener.close();
    reef.close();
    LOG.log(Level.FINEST, "Close launcher: done");
  }

  /**
   * Submit REEF job asynchronously and do not wait for its completion.
   *
   * @param driverConfig configuration of hte driver to submit to the RM.
   * @return ID of the new application.
   */
  public String submit(final Configuration driverConfig, final long waitTime) {
    reef.submit(driverConfig);
    waitForStatus(waitTime, LauncherStatus.SUBMITTED);
    return jobId;
  }

  /**
   * Wait for one of the specified statuses of the REEF job.
   * This method is called after the job is submitted to the RM via submit().
   * @param waitTime wait time in milliseconds.
   * @param statuses array of statuses to wait for.
   * @return the state of the job after the wait.
   */
  private LauncherStatus waitForStatus(final long waitTime, final LauncherStatus... statuses) {

    final long endTime = System.currentTimeMillis() + waitTime;

    final HashSet<LauncherStatus> statSet = new HashSet<>(statuses.length * 2);
    Collections.addAll(statSet, statuses);
    Collections.addAll(statSet, LauncherStatus.FAILED, LauncherStatus.FORCE_CLOSED);

    LOG.log(Level.FINEST, "Wait for status: {0}", statSet);
    final LauncherStatus finalStatus;

    synchronized (this) {
      while (!statSet.contains(status)) {
        try {
          final long delay = endTime - System.currentTimeMillis();
          if (delay <= 0) {
            break;
          }
          LOG.log(Level.FINE, "Wait for {0} milliSeconds", delay);
          this.wait(delay);
        } catch (final InterruptedException ex) {
          LOG.log(Level.FINE, "Interrupted: {0}", ex);
        }
      }

      finalStatus = status;
    }

    LOG.log(Level.FINEST, "Final status: {0}", finalStatus);
    return finalStatus;
  }

  /**
   * Run a job with a waiting timeout after which it will be killed, if it did not complete yet.
   *
   * @param driverConfig the configuration for the driver. See DriverConfiguration for details.
   * @param timeOut timeout on the job.
   * @return the state of the job after execution.
   */
  public LauncherStatus run(final Configuration driverConfig, final long timeOut) {

    final long startTime = System.currentTimeMillis();

    reef.submit(driverConfig);
    this.waitForStatus(timeOut - System.currentTimeMillis() + startTime, LauncherStatus.COMPLETED);

    if (System.currentTimeMillis() - startTime >= timeOut) {
      LOG.log(Level.WARNING, "The Job timed out.");
      synchronized (this) {
        this.status = LauncherStatus.FORCE_CLOSED;
      }
    }

    try {
      commandListener.close();
    } catch (Exception e) {
      // ignore
    }
    reef.close();
    return this.getStatus();
  }

  /**
   * @return the current status of the job.
   */
  public synchronized LauncherStatus getStatus() {
    return status;
  }

  /**
   * Update job status and notify the waiting thread.
   */
  private synchronized void setStatusAndNotify(final LauncherStatus newStatus) {
    LOG.log(Level.FINEST, "Set status: {0} -> {1}", new Object[] {status, newStatus});
    status = newStatus;
    notify();
  }

  @Override
  public String toString() {
    return String.format("DriverLauncher: { jobId: %s, status: %s }", jobId, status);
  }

  /**
   * Job driver notifies us that the job has been submitted to the Resource Manager.
   */
  public final class SubmittedJobHandler implements EventHandler<SubmittedJob> {
    @Override
    public void onNext(final SubmittedJob job) {
      LOG.log(Level.INFO, "REEF job submitted: {0}.", job.getId());
      jobId = job.getId();
      setStatusAndNotify(LauncherStatus.SUBMITTED);
    }
  }

  /**
   * Job driver notifies us that the job is running. {@link CommandListener} registers running job.
   */
  public final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.INFO, "The Job {0} is running.", job.getId());
      theJob = job;
      setStatusAndNotify(LauncherStatus.RUNNING);
      commandListener.setReefJob(theJob);
    }
  }

  /**
   * Job driver notifies us that the job had failed.
   */
  public final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      final Optional<Throwable> ex = job.getReason();
      LOG.log(Level.SEVERE, "Received an error for job " + job.getId(), ex);
      theJob = null;
      setStatusAndNotify(LauncherStatus.failed(ex));
      commandListener.setReefJob(null);
    }
  }

  /**
   * Job driver notifies us that the job had completed successfully.
   */
  public final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "The Job {0} is done.", job.getId());
      theJob = null;
      setStatusAndNotify(LauncherStatus.COMPLETED);
      commandListener.setReefJob(null);
    }
  }

  /**
   * Handler an error in the job driver.
   */
  public final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Received a resource manager error", error.getReason());
      theJob = null;
      setStatusAndNotify(LauncherStatus.failed(error.getReason()));
      commandListener.setReefJob(null);
    }
  }
}
