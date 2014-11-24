package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.parameters.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.*;

import javax.inject.Inject;

public final class FlexionConfiguration extends ConfigurationModuleBuilder {
  public final static RequiredParameter<String> IDENTIFIER = new RequiredParameter<>();
  public final static RequiredImpl<UserControllerTask> CONTROLLER_TASK = new RequiredImpl<>();
  public final static RequiredImpl<UserComputeTask> COMPUTE_TASK = new RequiredImpl<>();
  public final static RequiredParameter<Integer> EVALUATOR_NUM = new RequiredParameter<>();
  public final static OptionalParameter<Integer> EVALUATOR_SIZE = new OptionalParameter<>();
  public final static OptionalParameter<String> INPUT_DIR = new OptionalParameter<>();
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

}
