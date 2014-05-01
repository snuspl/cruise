package org.apache.reef.inmemory;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.client.CompletedJob;
import com.microsoft.reef.client.FailedJob;
import com.microsoft.reef.client.FailedRuntime;
import com.microsoft.reef.client.JobMessage;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.client.RunningJob;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

@Public 
@Provided
@ClientSide
@Unit
public final class InMemoryClient {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(InMemoryClient.class.getName());
  
  /**
   * Codec to translate messages to and from the job driver
   */
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  
  private LauncherStatus status = LauncherStatus.INIT;

  private RunningJob runningJob = null;

  private final REEF reef;

  @Inject
  InMemoryClient(final REEF reef) throws BindException {
    this.reef = reef;
  }

  /**
   *  Receive notification from the job driver that the job is running.
   */
  public final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(RunningJob job) {
      synchronized (InMemoryClient.this) {
        LOG.log(Level.INFO, "Run!");
        runningJob = job;
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
   * Receive notification from the job driver that the job had failed.
   */
  public final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(FailedJob job) {
      final Throwable ex = job.getCause();
      LOG.log(Level.SEVERE, "Received an error for job " + job.getId(), ex);
      runningJob = null;
      setStatusAndNotify(LauncherStatus.FAILED(ex));
    }
  }
  
  /**
   * Receive notification from the job driver that the job had completed successfully.
   */
  public final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(CompletedJob job) {
      LOG.log(Level.INFO, "Completed job: {0}", job.getId());
      runningJob = null;
      setStatusAndNotify(LauncherStatus.COMPLETED);
    }
  }
 
  /**
   * Receive notification that there was an exception thrown from the driver.
   */
  public final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Error in job driver: " + error, error.getCause());
      setStatusAndNotify(LauncherStatus.FAILED(error.getCause()));
    }
  }

  /**
   *  Wait until the job is done
   */
  public LauncherStatus run(final Configuration driverConfig) {
    this.reef.submit(driverConfig);
    synchronized (this) {
      while(!this.status.isDone()) {
        try {
          LOG.info("Waiting for the Driver to complete.");
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
   * Build a Client Configuration
   * @throws InjectionException 
   */
  public static InMemoryClient getClient(final Configuration runtimeConfiguration) throws BindException, InjectionException {
    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, FailedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, JobMessageHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, RuntimeErrorHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newInjector(runtimeConfiguration, clientConfiguration)
        .getInstance(InMemoryClient.class);
  }
  
  
  /**
   * @return the current status of the job.
   */
  public LauncherStatus getStatus() {
    return this.status;
  }

  /**
   * Update job status and notify the waiting thread.
   */
  public synchronized void setStatusAndNotify(final LauncherStatus status) {
    LOG.log(Level.FINEST, "Set status: {0} -> {1}", new Object[]{this.status, status});
    this.status = status;
    this.notify();
  }
}
