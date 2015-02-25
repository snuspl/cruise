package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.parameters.*;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.*;

import java.io.IOException;

public final class FlexionConfiguration extends ConfigurationModuleBuilder {
  public final static RequiredParameter<String> IDENTIFIER = new RequiredParameter<>();
  public final static RequiredImpl<UserControllerTask> CONTROLLER_TASK = new RequiredImpl<>();
  public final static RequiredImpl<UserComputeTask> COMPUTE_TASK = new RequiredImpl<>();
  public final static RequiredParameter<Integer> EVALUATOR_NUM = new RequiredParameter<>();
  public final static OptionalParameter<Integer> EVALUATOR_SIZE = new OptionalParameter<>();
  public final static RequiredParameter<String> INPUT_DIR = new RequiredParameter<>();
  public final static OptionalParameter<Boolean> ON_LOCAL = new OptionalParameter<>();
  public final static OptionalParameter<Integer> TIMEOUT = new OptionalParameter<>();

  public final static ConfigurationModule CONF = new FlexionConfiguration()
      .bindNamedParameter(JobIdentifier.class, IDENTIFIER)
      .bindImplementation(UserControllerTask.class, CONTROLLER_TASK)
      .bindImplementation(UserComputeTask.class, COMPUTE_TASK)
      .bindNamedParameter(EvaluatorNum.class, EVALUATOR_NUM)
      .bindNamedParameter(EvaluatorSize.class, EVALUATOR_SIZE)
      .bindNamedParameter(InputDir.class, INPUT_DIR)
      .bindNamedParameter(OnLocal.class, ON_LOCAL)
      .bindNamedParameter(Timeout.class, TIMEOUT)
      .build();

  public final static ConfigurationModule CONF(final String args[]) throws IOException, InjectionException {

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);

    cl.registerShortNameOfClass(EvaluatorSize.class);
    cl.registerShortNameOfClass(OnLocal.class);
    cl.registerShortNameOfClass(EvaluatorNum.class);
    cl.registerShortNameOfClass(Timeout.class);
    cl.registerShortNameOfClass(InputDir.class);
    cl.processCommandLine(args);

    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());

    return CONF
        .set(EVALUATOR_SIZE, injector.getNamedInstance(EvaluatorSize.class))
        .set(ON_LOCAL, injector.getNamedInstance(OnLocal.class))
        .set(EVALUATOR_NUM, injector.getNamedInstance(EvaluatorNum.class))
        .set(TIMEOUT, injector.getNamedInstance(Timeout.class))
        .set(INPUT_DIR, injector.getNamedInstance(InputDir.class));
  }

}
