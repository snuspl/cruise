`StatePassing` is a simple test that shows how state is passed across Tasks via `Service` that lives in the `Context` of an `Evaluator`. You can find the code in [`reef-tests`](https://github.com/Microsoft-CISL/REEF/tree/master/reef-tests/src/main/java/com/microsoft/reef/tests/statepassing)

	Services: these are user code that lives in the context of an Evaluator, therefore outliving any particular Task. Examples include caches of parsed data, intermediate state and network connection pools.

This tutorial explains how to create a Service and how the Service works.

## Prerequisite
* `HelloREEF Tutorial` : The most part of this example is quite similar to `HelloREEF` except Service. You can learn about the basic components(Task, Driver, Handler, ...) from the HelloREEF tutorial.

## Define a class

The class `Counter` will be used as a Service. 
It is a simple counter as you see.

```
public class Counter {

  private int value = 0;

  @Inject
  public Counter() {
  }

  public void increment() {
    this.value += 1;
  }

  public int getValue() {
    return value;
  }
}
```


## Register a Service (Driver-side Logic)

To register a Service in Context, you need to build service configuration and submit it to the evaluator.

1. In `EventHandler<AllocatedEvaluator>`, build a ServiceConfiguration.

	```
    final Configuration serviceConfiguration = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, Counter.class)
        .build();
	```

* Submit the `ServiceConfiguration` with the `ContextConfiguration` to the evaluator.

	```
public void onNext(final AllocatedEvaluator eb) {
	...
	eb.submitContextAndService(contextConfiguration, serviceConfiguration)
	...
}
	```

## StatePassingTask

### Inject the Service to StatePassingTask

You can use Service by injecting `Counter` class to the constructor of StatePassingTask. 

```
  private final Counter c;

  @Inject
  public StatePassingTask(final Counter c) {
    this.c = c;
  }
```

### Call

`call()` increments the value of Counter and returns an array created by `makeArray()`.

```
  public byte[] call(byte[] memento) throws Exception {
    this.c.increment();
    return makeArray(this.c.getValue(), (byte) 1);
  }
```

`makeArray()` builds an array filled with 1's as many as the value of `Counter`.

```
  private static byte[] makeArray(final int size, final byte value) {
    final byte[] result = new byte[size];
    Arrays.fill(result, value);
    return result;
  }
```

## Handle Task Completion (Driver-side Logic)
When a Task completes, `EventHandler<CompletedTask>` decides the next action to perform.
If the Task has not executed enough times(`PASSES`), it calls `netxtPass()` with the active context.

```
      if (pass < PASSES) {
        LOG.log(Level.INFO, "Submitting the next Task");
        nextPass(completed.getActiveContext());
      } else {
        LOG.log(Level.INFO, "Done");
        completed.getActiveContext().close();
      }
```

`nextPass()` increments `pass`, and submit another Task to the active context. Then the new Task has the `Counter` with the latest updated value.

```
  private void nextPass(final ActiveContext activeContext) {
    try {
      activeContext.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "StatePassing-" + pass)
          .set(TaskConfiguration.TASK, StatePassingTask.class)
          .build());
      ++pass;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }
```
