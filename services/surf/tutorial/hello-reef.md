## From the REEF tutorial

## Components of the REEF application

Each REEF application consists of four main components:
 * **Client:** The client runs on the gateway host; it is usually started from
   the `main()` method in Java. Client starts the Driver on YARN.  For simple
   tasks, like HelloREEF, we use a higher level class `DriverLauncher` that
   implements most of the generic Client functionality.
 * **Driver:** runs on the YARN container, and can request multiple Evaluators
   to run Tasks on. The driver controls the tasks and communicates with the job
   Client.
 * **Evaluator:** reusable environment to run Tasks on. It can be viewed as
   REEF abstraction around the YARN Container. Evaluator can retain the data
   shared across the Tasks that run on that evaluator.
 * **Task:** is a unit of work on REEF, submitted by job Driver to the
   Evaluators.

Below we will review each component's role in HelloREEF application.

## Job Client and the Launcher

[`HelloREEF`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/hello/HelloREEF.java)
and
[`HelloReefYarn`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/hello/HelloReefYarn.java)
contain the client part of the application. In the `main()` method, we create
either the local configuration (for test run on the local PC):

```java
final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
    .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
    .build();
```

or the configuration to run on YARN:

```java
final Configuration runtimeConfiguration = YarnClientConfiguration.CONF
    .set(YarnClientConfiguration.REEF_JAR_FILE, EnvironmentUtils.getClassLocationFile(REEF.class))
    .build();
```

We also configure the job driver:

```java
final Configuration driverConfiguration =
    EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
      .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
    .build();
```

Here we specify events the Driver should handle, and also say which JARs
have to be copied to the remote (in the `EnvironmentUtils.addClasspath()`
method).

After that we supply `runtimeConfiguration` and `driverConfiguration` to the
`DriverLauncher`:

```java
DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, JOB_TIMEOUT);
```

`DriverLauncher` implements the basic Client functionality: it submits Driver
configuration to the REEF framework, waits for the job to complete, and handles
the communication with the job driver.


## Job Driver

Job driver for HelloREEF application is implemented in the
[`HelloDriver`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/hello/HelloDriver.java)
class. Its constructor is very simple:

```java
@Inject
public HelloDriver(final EvaluatorRequestor requestor) {
  this.requestor = requestor;
}
```

Note the `@Inject` annotation in the constructor: it means that `HelloDriver`
object is instantiated automatically by REEF (or, more accurately, by
[TANG](https://github.com/Microsoft-CISL/TANG), dependency injection framework
used in REEF). The framework also injects the constructor parameters.

As we've seen from the `driverConfiguration`, HelloREEF Driver responds to two
events:
 * `ON_DRIVER_STARTED`: Driver gets this event when it and its environment has
   been fully constructed and ready to run.
 * `ON_EVALUATOR_ALLOCATED`: Driver receives one `AllocatedEvaluator` message
   for each Evaluator successfully allocated for it.

Event handlers in REEF implement the `EventHandler` interface, which has only
one method, `.onNext()`. Both handlers in the HelloREEF example are implemented
as inner classes of `HelloDriver`, and are instantiated by the REEF framework.

**Important:** Note that the `HelloDriver` class has the `@Unit` annotation. It
tells [TANG](https://github.com/Microsoft-CISL/TANG) (dependency injection
framework used in REEF) to instantiate `HelloDriver` and its inner classes as a
single unit.

Below is the HelloREEF implementation of these event handlers:

```java
final class StartHandler implements EventHandler<StartTime> {
  @Override
  public void onNext(final StartTime startTime) {
    HelloDriver.this.requestor.submit(
        EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setSize(EvaluatorRequest.Size.SMALL)
          .build());
  }
}
```

That is, HelloREEF driver requests one small instance of the Evaluator to run
the Task on.

Upon successful Evaluator allocation, Driver receives the `AllocatedEvaluator`
message, and can use it submit a new Task to the corresponding Evaluator:

```java
final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
  @Override
  public void onNext(final AllocatedEvaluator allocatedEvaluator) {
    try {
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "HelloREEFContext")
          .build();
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "HelloREEFTask")
          .set(TaskConfiguration.TASK, HelloTask.class)
          .build();
      allocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
    } catch (final BindException ex) {
      throw new RuntimeException("Unable to setup Task or Context configuration.", ex);
    }
  }
}
```

Note that to start the Task, we submit *two* pieces of configuration to the
Evaluator: one for the Task and another for the Context.

Task configuration is similar to one of the Driver: it simply specifies the
Task implementation class.

The Context is a data structure that resides on the Evaluator side and can hold
data across multiple Task executions. HelloREEF application runs just one
task, so the Context configuration in this case is minimal.

All other events that the Driver can receive (e.g. `FailedTask` and such) are
processed by default handlers supplied by REEF.


## Task

[`HelloTask`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/hello/HelloTask.java)
class is where the application actually performs the computation:

```java
final class HelloTask implements Task {

  @Inject
  HelloTask() {
  }

  @Override
  public final byte[] call(final byte[] memento) {
    System.out.println("Hello, REEF!");
    return null;
  }
}
```

Main Task method is `.call()`. For HelloREEF, it simply prints the greeting
(that will go to stdout of the corresponding Evaluator container), and quits.

Just like the Driver, Task *must* have a constructor annotated with
`@Inject`, so the class can be instantiated by the dependency injection
framework.

Task can accept and return a value, encoded as byte array (REEF
provides tools for encoding/decoding):
 * `.call()` method argument (`memento`) usually serves as a restart point for
   tasks that can be suspended, restarted, and/or moved from one Evaluator to
   another. For simple applications, like HelloREEF, it can be ignored.
 * Return value (also a byte array), is delivered by REEF to the Driver as part
   of the `CompletedTask` event.
 * Note that we typically don't use `memento` to supply input parameters to the
   Task. Preferred way to pass parameters to the Task is to add them to the
   Task or Context configuration, so they can be injected into the
   Task constructor. (e.g. see the [Distributed Shell Tutorial](https://github.com/Microsoft-CISL/REEF/wiki/Distributed-Shell-Tutorial)).

## Running the application

### Testing it locally

The easiest way to run HelloREEF in local mode is to use the maven profile in
[`reef-examples`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/pom.xml#L60):

```
mvn -PHelloREEF
```

That will run
[`HelloREEF.main()`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/hello/HelloREEF.java#L62)
method.

### Running on YARN

To run on YARN, we have to invoke
[`HelloReefYarn.main()`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/hello/HelloReefYarn.java#L50),
and also add YARN JAR files and `$YARN_CONF_DIR` to the class path. Shell
script that does that is in
[`REEF/bin/hello.sh`](https://github.com/Microsoft-CISL/REEF/blob/master/bin/hello.sh)

