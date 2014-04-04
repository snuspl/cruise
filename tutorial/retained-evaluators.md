# Retained Evaluators
Retained Evaluators receives a shell command as input from user, execute them and return the result back to user.

##Launch

`Launch` is the main class of this application. In this class, the configurations for runtime and client are built up and the application starts from here.

This application receives four arguments from command line.

* Command : The shell command
* NumRuns : Number of times to run the command
* NumEval : Number of evaluators to request
* Local : Whether or not to run on the local runtime

The arguments are injected via `TANG` and we will see how it can be done.

```
@NamedParameter(doc = "The shell command", short_name = "cmd", default_value = "*INTERACTIVE*")
public static final class Command implements Name<String> {
}
```
We make a class for each argument. This class has an empty body and just implements `Name<T>` interface, where `T` is the type of argument. For example, `NumRuns` implements `Name<Integer>` and `NumLocal` implements `Name<Boolean>`.

`@NamedParameter` annotation provides more information to TANG. By this annotation, you can set the default value of the argument and give some useful information for documentation.


In `getClientConfiguration()` method, it merges `runtimeConfiguration`, `clientConfiguration` and `commandLindConf`.

```
return Tang.Factory.getTang()
    .newConfigurationBuilder(runtimeConfiguration, clientConfiguration,
        cloneCommandLineConfiguration(commandLineConf))
    .build();

```

* `runtimeConfiguration` determines whether this application is executed in a local mode or remote mode. 

	```
	final Configuration runtimeConfiguration;
	if (isLocal) {
	  LOG.log(Level.INFO, "Running on the local runtime");
	  runtimeConfiguration = LocalRuntimeConfiguration.CONF
	      .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
	      .build();
	} else {
	  LOG.log(Level.INFO, "Running on YARN");
	  runtimeConfiguration = YarnClientConfiguration.CONF.build();
	}
	```

* `clientConfiguration` contains the handlers for each event related to job.
	
	```
	final Configuration clientConfiguration = ClientConfiguration.CONF
	   .set(ClientConfiguration.ON_JOB_RUNNING, JobClient.RunningJobHandler.class)
	   .set(ClientConfiguration.ON_JOB_MESSAGE, JobClient.JobMessageHandler.class)
	   .set(ClientConfiguration.ON_JOB_COMPLETED, JobClient.CompletedJobHandler.class)
	   .set(ClientConfiguration.ON_JOB_FAILED, JobClient.FailedJobHandler.class)
	   .set(ClientConfiguration.ON_RUNTIME_ERROR, JobClient.RuntimeErrorHandler.class)
	   .build();
	```
 
* `commandLineConfig` has the command line arguments and these would be handed to the client by injection.


	```
	final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
	final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
	    cb.bindNamedParameter(Command.class, injector.getNamedInstance(Command.class));
	    cb.bindNamedParameter(NumRuns.class, String.valueOf(injector.getNamedInstance(NumRuns.class)));
	    cb.bindNamedParameter(NumEval.class, String.valueOf(injector.getNamedInstance(NumEval.class)));
	return cb.build();
	```


## Job Client
Client launches the job driver and submits tasks to the driver. 

```
@Inject
JobClient(final REEF reef,
    @Parameter(Launch.Command.class) final String command,
    @Parameter(Launch.NumRuns.class) final Integer numRuns,
    @Parameter(Launch.NumEval.class) final Integer numEvaluators) throws BindException {
```
When we built the client configuration in the `Launch.java`, we set the parameters. By this, the parameters are injected automatically when the constructor of JobClient is called.

Now we set the driver configuration, setting the event handlers which are handled by Job driver.

```
final JavaConfigurationBuilder configBuilder = Tang.Factory.getTang().newConfigurationBuilder();
configBuilder.addConfiguration(
    EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "eval-" + System.currentTimeMillis())
        (...More handlers...)
      .set(DriverConfiguration.ON_DRIVER_STOP, JobDriver.StopHandler.class)
    .build());
configBuilder.bindNamedParameter(Launch.NumEval.class, "" + numEvaluators);
this.driverConfiguration = configBuilder.build();
```

`submit()` method launches the job driver with those configuration built above.

```
public void submit() {
  this.reef.submit(this.driverConfiguration);
}
```

Commands are submitted by this `submitTask()` method. When in `Non-interactive mode`, then the `cmd` created by argument is used. Otherwise, the client wait for the user's input and submit a task when a command is requested.

```
private synchronized void submitTask(final String cmd) {
    LOG.log(Level.INFO, "Submit task {0} \"{1}\" to {2}",
        new Object[]{this.numRuns + 1, cmd, this.runningJob});
    this.startTime = System.currentTimeMillis();
    this.runningJob.send(CODEC.encode(cmd));
```

## ShellTask
Shell Task simply executes the shell command given, and return the result back to the driver. 
This task initializes a `Process` to exeucte the command, and collects the result as a `String` until there is no more line.

```
final Process proc = Runtime.getRuntime().exec(cmd);
try (final BufferedReader input =
    new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
    String line;
    while ((line = input.readLine()) != null) {
        sb.append(line).append('\n');
    }
}
```
The result is returned as a byte array by `ObjectSerializableCodec`, which encode a String to be a byte array.

```
    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();
    return codec.encode(sb.toString());
```

## Job Driver
`Job Driver` keeps tracking the state of multiple evaluators and takes control of tasks. `Job Driver` has four states, which are:

* `INIT` : Initial state, ready to request the evaluators.
* `WAIT_EVALUATORS` : Wait for requested evaluators to initialize.
* `READY` : Ready to submitTask a new task.
* `WAIT_TASKS` : Wait for tasks to complete.

Also, it has an Object which maps from context ID to running evaluator context.

```
private final Map<String, ActiveContext> contexts = new HashMap<>();
```

On the EventHandler of `ActiveContext` event, the driver puts the newly activated context into this mapping.

```
public void onNext(final ActiveContext context) {
  	...
    JobDriver.this.contexts.put(context.getId(), context);
    ...
}
```
When a task is successfully completed, the driver checks whether all the tasks are done. If there is no running task, the driver returns the result and get ready to submit another command.
  
 ```
final class CompletedTaskHandler implements EventHandler<CompletedTask> {
  @Override
  public void onNext(final CompletedTask task) {
      if (--JobDriver.this.expectCount <= 0) {
        JobDriver.this.returnResults();
        JobDriver.this.state = State.READY;
        if (JobDriver.this.cmd != null) {
          JobDriver.this.submit(JobDriver.this.cmd);
        }
      }
    }
  }
}
```
`returnResults()` method shows hwo the result is generated and forwarded to the `Client`. After the result is collected across the `Evaluators` and encoded to be a byte array, it is passed to `jobMessageObserver` as an argument of the `onNext()` method.

```
  private void returnResults() {
    final StringBuilder sb = new StringBuilder();
    for (final String result : this.results) {
      sb.append(result);
    }
    this.results.clear();
    LOG.log(Level.INFO, "Return results to the client:\n{0}", sb);
    this.jobMessageObserver.onNext(CODEC.encode(sb.toString()));
  }
```

If there happended an error on `Evaluator` or `Context`, then the driver take actions to the failure and report to the `Client`.

* On a failure of `Evaluator`, the driver removes all the failed Context in the Evaluator from the active context list(`contexts`).

	``` 
     synchronized (JobDriver.this) {
      LOG.log(Level.SEVERE, "FailedEvaluator", eval);
      for (final FailedContext failedContext : eval.getFailedContextList()) {
        JobDriver.this.contexts.remove(failedContext.getId());
      }
	```
	
	And the driver reports the failure to the client with `onError()` method.
	
	```
	JobDriver.this.jobMessageObserver.onError(eval.getEvaluatorException());
	```
	
* When a `Context` had failed, remove the failed Context from the active context list as well.

	```
	synchronized (JobDriver.this) {
      JobDriver.this.contexts.remove(context.getId());
    }
	```
	In this case, the driver also reports the failure as above.
	
	```
    JobDriver.this.jobMessageObserver.onError(context.asError());
    ```
