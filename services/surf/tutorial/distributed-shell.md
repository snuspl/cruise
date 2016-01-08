## Copied from the REEF tutorial

## Introduction

This tutorial describes the distributed shell implementation located in
[`REEF/reef-examples`](https://github.com/Microsoft-CISL/REEF/tree/master/reef-examples).
The following concepts will be covered.

* Launching a REEF client
* Launching a REEF driver
* Receiving job messages at the client
* Requesting REEF evaluators and launching tasks
* The REEF supplied resource catalog

## Executing Distributed Shell

The `bin` directory contains a bash script that will execute the distributed
shell program on either runtime. It has a single required argument:
`-cmd **shell command**`

By default, this will execute the given shell command on the local runtime. If
you want to execute on YARN, then you will need to include the optional
argument: `-local false`

You may also specify files to be uploaded and made available to the executing
shell command environment e.g., a python script.

* Make sure you have set `REEF_HOME`:
```sh
      export REEF_HOME='REEF directory'
```

* Execute uptime on local runtime:
```text
      $REEF_HOME/bin/shell.sh -cmd uptime

      Distributed shell result:
      Node 10.0.1.24:
       6:35  up 14:26, 2 users, load averages: 1.92 1.76 1.62
```

* Execute uptime on YARN runtime:
```text
      $REEF_HOME/bin/shell.sh -cmd uptime -local false

      <similar output>
```

## Distributed Shell Code

Located in
[`DSClient.java`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/ds/DSClient.java),
the class accepts command line arguments (e.g., what shell command to run) and
launches a REEF client, which is defined in
[`DistributedShell.java`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/ds/DistributedShell.java).

The following code snippet configures the REEF client to reference
[`DistributedShell.java`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/ds/DistributedShell.java)
for the job observer (to receive job messages) and runtime error handler:

```java
final Configuration clientConfiguration = ClientConfiguration.CONF
    .set(ClientConfiguration.JOB_OBSERVER, DistributedShell.class)
    .set(ClientConfiguration.RUNTIME_ERROR_HANDLER, DistributedShell.class)
    .build();
```

### DistributedShell Client

The construction of the `DistributedShell` class injects
(see [TANG](https://github.com/Microsoft-CISL/TANG)) the REEF client runtime, which
can be used to submit drivers. The following method describes
this behavior.

```java
public void submit(final String cmd) throws BindException {
    final String jobid = "distributed-shell";

    ConfigurationModule driverConf = DriverConfiguration.CONF
        .set(DC.DRIVER_IDENTIFIER, jobid)
        .set(DC.ON_TASK_COMPLETED, DSJD.CompletedTaskHandler.class)
        .set(DC.ON_EVALUATOR_ALLOCATED, DSJD.AllocatedEvaluatorHandler.class)
        .set(DC.ON_DRIVER_STARTED, DSJD.StartHandler.class)
        .set(DC.ON_DRIVER_STOP, DSJD.StopHandler.class);

    driverConf = EnvironmentUtils.addClasspath(driverConf, DC.GLOBAL_LIBRARIES);
    driverConf = EnvironmentUtils.addAll(driverConf, DC.GLOBAL_FILES, this.resources);

    final JavaConfigurationBuilder cb =
        Tang.Factory.getTang().newConfigurationBuilder(driverConf.build());
    cb.bindNamedParameter(DSClient.Command.class, cmd);

    this.reef.submit(cb.build());
  }
```
* **Note:** We use the following abbreviations in the text and code:
    - DC = `DriverConfiguration`
    - DSJD = `DistributedShellJobDriver`

The submit method sets up the driver configuration and submits the
configuration to the reef instance. The job identifier is set to some arbitrary
string. The code sets various handlers (allocated/completed task,
start/stop driver) to implementations in `DistributedShellJobDriver` (DSJD).
Global files are also included in the job driver using a utility that adds
everything in the classpath to the global libraries, and anything included in
the command line files parameter (`-files file1:file2:...`) to global files.
Note: these files will be included in the Evaluator directory, accessible to
any context/task implementations. Finally, we include the command line
argument by creating a `JavaConfigurationBuilder` (see
[TANG](https://github.com/Microsoft-CISL/TANG)) that we can use to
define the `DSClient.Command.class` binding along with the driver configuration.

### DistributedShellJobDriver

[`DistributedShell.java`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/ds/DistributedShell.java)
launches the
[`DistributedShellJobDriver.java`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/ds/DistributedShellJobDriver.java)
(DSJD) on the master resource i.e., application master in YARN. The following
lists (a version of) the DSJD constructor:

```java
/**
   * Distributed Shell job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param clock Wake clock to schedule and check up running jobs.
   * @param client is used to send messages back to the client.
   * @param evaluatorRequestor is used to request Evaluators.
   * @param cmd shell command to run.
   */
  @Inject
  DistributedShellJobDriver(final Clock clock,
                            final JobMessageObserver client,
                            final EvaluatorRequestor evaluatorRequestor,
                            final @Parameter(DSClient.Command.class) String cmd) {
    this.clock = clock;
    this.client = client;
    this.evaluatorRequestor = evaluatorRequestor;
    this.cmd = cmd;
    this.catalog = evaluatorRequestor.getResourceCatalog();
  }
```

The clock instance can be used to shutdown the driver (see `clock.close()` in
[Wake](https://github.com/Microsoft-CISL/Wake)). The client instance (of
`JobMessageObserver`) can be used to communicate
with the `DistributedShell` client via the `.onNext(JobMessage)` method. We will use
this (in the DSJD `StopHandler` to send back the final result. The
`evaluatorRequestor` instance (of `EvaluatorRequestor`) can be used to request
evaluators from REEF, which will be translated down to the underlying resource
management layer (e.g., local or YARN). Finally, the desired _cmd_ argument is
supplied.

The constructor implementation is straight forward: initialize each instance
variable. The last instance variable references the (REEF supplied) resource
catalog, which contains node and rack level information about the cluster;
supplied by the resource management layer. We will use the catalog information
to allocate a resource on each node.

#### Start Event

The first event that gets triggered by REEF is the `StartTime` event. It is also
interesting to note that a driver is required to implement a handler for this
event. All other events can be processed by default handlers provided by REEF.
The following lists the start handler defined in DSJD.

```java
  /**
   * Event handler that signal the start of execution.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "START TIME: {0}", startTime);
      schedule();
    }
  }
```

The implementation of this handler logs the event and calls the `.schedule()`
method shown here:

```java
  /**
   * Request evaluators on each node.
   */
  private void schedule() {
    final int numNodes = this.catalog.getNodes().size();
    if (numNodes > 0) {
      LOG.log(Level.INFO, "Schedule on {0} nodes.", numNodes);
      try {
        for (final NodeDescriptor node : this.catalog.getNodes()) {
          this.evaluatorRequestor.submit(
              EvaluatorRequest.newBuilder()
                  .fromDescriptor(node)
                  .setSize(EvaluatorRequest.Size.SMALL)
                  .setNumber(1).build());
        }
      } catch (final Exception ex) {
        LOG.log(Level.SEVERE, "submitTask() failed", ex);
        throw new RuntimeException(ex);
      }
    } else {
      this.clock.scheduleAlarm(CHECK_UP_INTERVAL,
          new EventHandler<Alarm>() {
            @Override
            public void onNext(final Alarm time) {
              LOG.log(Level.INFO, "Alarm: {0}", time);
              schedule();
            }
          });
    }
  }
```

The schedule method relies on the fact that the catalog has been populated,
which may not happen immediately due to asynchronous messaging. That is, when
REEF starts up on YARN, it will make an asynchronous request to the YARN
resource manager for cluster node information, which returns this information
to REEF sometime in the future. Instead of waiting for this information to come
back, REEF starts the driver. If the driver needs this information in order to
function, then it must implement the waiting logic, as we have done here in the
`.schedule()` method. Specifically, the **else** case uses the _clock_ to schedule
an alarm that will call the schedule method again at some future time;
hopefully with the complete catalog information. Given a complete catalog
(shown here as a simple check `numNodes > 0`; note the complete catalog
information is returned in a single message by the YARN RM) the DSJD requests a
single resource on each node.

#### Allocated Evaluator

The DSJD defines a handler that receives `AllocatedEvaluator` events; it gets
one event for each evaluator requested in the `.schedule()` method. The
following listing shows this handler implementation:

```java
 /**
   * Receive notification that an Evaluator had been allocated,
   * and submitTask a new Task in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      try {
        LOG.log(Level.INFO, "Allocated Evaluator: {0}", eval.getId());
        // Submit a Task that executes the shell command in this Evaluator

        final JavaConfigurationBuilder taskConfigurationBuilder =
              Tang.Factory.getTang().newConfigurationBuilder();
        taskConfigurationBuilder.bindNamedParameter(
            DSClient.Command.class, DistributedShellJobDriver.this.cmd);
        taskConfigurationBuilder.addConfiguration(
            TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, eval.getId() + "_shell_task")
                .set(TaskConfiguration.TASK, ShellTask.class)
                .build());

        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "DS")
            .build();

        eval.submitContextAndTask(contextConfiguration, taskConfigurationBuilder.build());
      } catch (final BindException e) {
        throw new RuntimeException(e);
      }
    }
  }
```

In general, the `AllocatedEvaluator` interface allows the following actions:
* Add (local) file resources. Note: we are using global file resources, which
  are automatically copied to the Evaluator environment.
* Submit a root context; will receive a `ActiveContext`/`FailedContext` event.
* Submit a root context and a task; will receive a
  `RunningTask`/`FailedTask` event.

The DSJD `AllocatedEvaluatorHandler` shown above, first configures the shell
task, and then uses the `.submitContextAndTask()` method to submit both
a root context and a task. The DSJD will then expect to receive a
`RunningTask` (or `FailedTask` if something goes wrong) event at some
point in the future. The DSJD implementation will not receive an `ActiveContext`
event since a task was submitted along with the (root) context. Note: the
`RunningTask` (and `CompletedTask`) event instances contain handles to the
(root, in this case) `ActiveContext`. Lastly, since we do not really need a
handle to the `RunningTask` here, the DSJD ignores this implementation, and
lets REEF supply a default implementation that simply logs the
(`RunningTask`) event.

#### Shell Task

The shell task implementation is defined in
[`ShellTask.java`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/ds/ShellTask.java).
Its constructor has the **command** injected, which it executes in the
`Task.call()` method, shown here:

```java
  /**
   * Execute the shell command and return the result, which is sent back to
   * the JobDriver and surfaced in the CompletedTask object.
   * @param memento ignored.
   * @return byte string containing the stdout from executing the shell command.
   * @throws Exception when something goes wrong when executing the shell command.
   */
  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final StringBuilder sb = new StringBuilder();
    try {
      // Execute the command
      LOG.log(Level.INFO, "Call: {0} with: {1}", new Object[] {this.command, memento});
      final Process proc = Runtime.getRuntime().exec(this.command);
      try (final BufferedReader input = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
        String line;
        while ((line = input.readLine()) != null) {
          sb.append(line).append('\n');
        }
      }
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Error in call: " + this.command, ex);
      sb.append(ex);
    }
    // Return the result
    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();
    return codec.encode(sb.toString());
  }
```

After the shell command finishes, the `ShellTask` will package the result
into a byte array, and return it from the `.call()` method. REEF will
(**immediately!**) send this return result back to the driver, and surface it as
a `CompletedTask` event.

### Wrap it up

Back to the DSJD, which needs to collect the final result of all shell
tasks. It does this in the `CompletedTaskHandler` shown below:

```java

  /**
   * Receive notification that the Task has completed successfully.
   * Store the task results and close the Evaluator.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask act) {
      LOG.log(Level.INFO, "Completed task: {0}", act.getId());

      // Take the message returned by the task and add it to the running result.
      final String result = CODEC.decode(act.get());
      final NodeDescriptor node = act.getActiveContext().getNodeDescriptor();
      results.add("Node " + node.getName() + ":\n" + result);

      LOG.log(Level.INFO, "Task result: {0} on node {1}",
          new Object[] { result, node.getName() });

      act.getActiveContext().close();
    }
  }
```

The DSJD contains an internal list of results that it uses to append new
task output. When DSJD receives a `CompletedTask` event, it decodes the
output (string), adds the relevant node information, and appends the result. It
then closes the (root) active context, which automatically returns the
Evaluator resource to the underlying resource manager. When all tasks have
finished, the DSJD enters the idle state: meaning, that it is not holding any
resources, nor does it have any outstanding resource requests or alarms. When a
driver is in the idle state, REEF will automatically trigger a shutdown event.
Therefore, the DSJD does not need any explicit shutdown logic e.g., count how
many completed tasks have come back and explicitly call `clock.close()`
when all have been accounted for.

We still need to return the final result to the client. DSJD does that in the
(optional) driver stop handler.

#### Driver Stop Handler

A driver may optionally define a stop handler that is called on shutdown;
either due to an explicit `clock.close()` action, or when the driver is deemed
idle. The DSJD `StopHandler` uses this event to piece together the final output,
which contains the output of each task, shown here:

```java
  /**
   * Event handler signaling the end of the job.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      // Construct the final result and forward it to the JobObserver
      final StringBuilder sb = new StringBuilder();
      for (final String result : DistributedShellJobDriver.this.results) {
        sb.append('\n').append(result);
      }

      client.onNext(CODEC.encode(sb.toString()));
    }
  }
```

The client variable is an instance of the `JobMessageObserver`, supplied by REEF
and injected into the DSJD constructor. DSJD uses this instance to communicate
with the `DistributedShell` (client) via the `.onNext(JobMessage)` method, shown
here:

```java
  /**
   * Receive message from the DistributedShellJobDriver.
   * There is only one message, which comes at the end of the driver execution
   * and contains shell command output on each node.
   * This method is inherited from the JobObserver interface.
   */
  @Override
  public synchronized void onNext(final JobMessage message) {
    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();
    this.dsResult = codec.decode(message.get());
  }
```

The last event in the `DistributedShell` client is the `CompletedJob`, which in
our implementation notifies some waiting code that the result has been
received.

```java
  /**
   * Receive notification from DistributedShellJobDriver that the job had completed successfully.
   * This method is inherited from the JobObserver interface.
   */
  @Override
  public synchronized void onNext(final CompletedJob job) {
    LOG.log(Level.INFO, "Completed job: {0}", job.getId());
    this.notify();
  }
```


