package org.apache.reef.inmemory;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.client.CompletedJob;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.FailedJob;
import com.microsoft.reef.client.FailedRuntime;
import com.microsoft.reef.client.JobMessage;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.client.RunningJob;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;


public final class InMemoryClient {

  private static final Logger LOG = Logger.getLogger(InMemoryClient.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

  private final Configuration driverConfig;

  private LauncherStatus status = LauncherStatus.INIT;

  private boolean isBusy = true;
  private RunningJob theJob = null;

  private final REEF reef;

  @Inject
  InMemoryClient(final REEF reef) throws BindException {
    this.reef = reef;

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.addConfiguration(
        EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "InMemory")
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, InMemoryDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, InMemoryDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, InMemoryDriver.StartHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, InMemoryDriver.TaskMessageHandler.class)
        .build());

    this.driverConfig = cb.build();
  }
  
  /**
   * Submit driver configuration to launch the driver.
   */
  public void submit() {
    this.reef.submit(this.driverConfig);
  }
  
  /**
   *  Receive notification from the job driver that the job is running.
   */
  final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(RunningJob job) {
      synchronized (InMemoryClient.this) {
        LOG.log(Level.INFO, "Run!");
        theJob = job;
        isBusy = true;
        setStatusAndNotify(LauncherStatus.RUNNING);
      }
    }
  }
 
  /**
   * Receive messages from the driver.
   */
  public class JobMessageHandler implements EventHandler<JobMessage> {
      @Override
      public void onNext(JobMessage msg) {
        String result = CODEC.decode(msg.get());
        Long time = System.currentTimeMillis();
        System.out.println(result);
        LOG.log(Level.INFO, "JobMessageHandler: Task completed in {0} msec.:\n{1}",
            new Object[]{time, result});
      }
    }
  
  /**
   * Receive notification from the job driver that the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(CompletedJob job) {
      LOG.log(Level.INFO, "Completed job: {0}", job.getId());
      isBusy = false;
      stopAndNotify(LauncherStatus.COMPLETED);
    }
  }
 
  /**
   * Receive notification from the job driver that the job had failed.
   */
  final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(FailedJob job) {
      final Throwable ex = job.getReason().get();
      LOG.log(Level.SEVERE, "Failed job: {0}", job.getId());
      stopAndNotify(LauncherStatus.FAILED(ex));
    }
  }

  /**
   * Receive notification that there was an exception thrown from the driver.
   */
  public final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Error in job driver: " + error, error.getReason());
      stopAndNotify(LauncherStatus.FAILED);
    }
  }

  /**
   *  Wait until the job is done
   */
  public LauncherStatus waitForCompletion() {
    synchronized (this) {
      while(this.isBusy) {
        LOG.info("Waiting for the Driver to complete.");
        try {
          this.wait();
        } catch (final InterruptedException ex) {
          LOG.log(Level.WARNING, "Interrupted", ex);
        }
      }
    }
    LOG.log(Level.INFO, "Driver completed!");
    this.reef.close();
    return this.status;
  }

  /**
   *  Set status of Client and notify
   */
  public synchronized void setStatusAndNotify(final LauncherStatus status) {
    LOG.log(Level.FINEST, "Set status: {0} -> {1}", new Object[]{this.status, status});
    this.status = status;
    this.notify();
  }
  
  /**
   * Set status COMPLETED or FAILED and notify to finish the Application.
   */
  public synchronized void stopAndNotify(final LauncherStatus status) {
    this.theJob = null;
    setStatusAndNotify(status);
  }
}
