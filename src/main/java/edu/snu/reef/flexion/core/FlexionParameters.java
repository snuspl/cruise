package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

public final class FlexionParameters {
  private final String identifier;
  private final UserControllerTask userControllerTask;
  private final UserComputeTask userComputeTask;
  private final UserParameters userParameters;
  private final int evalNum;
  private final int evalSize;
  private final String inputDir;
  private final boolean onLocal;
  private final int timeout;

  @Inject
  private FlexionParameters(@Parameter(JobIdentifier.class) final String identifier,
                            final UserControllerTask userControllerTask,
                            final UserComputeTask userComputeTask,
                            final UserParameters userParameters,
                            @Parameter(EvaluatorNum.class) final int evalNum,
                            @Parameter(EvaluatorSize.class) final int evalSize,
                            @Parameter(InputDir.class) final String inputDir,
                            @Parameter(OnLocal.class) final boolean onLocal,
                            @Parameter(Timeout.class) final int timeout) {
    this.identifier = identifier;
    this.userControllerTask = userControllerTask;
    this.userComputeTask = userComputeTask;
    this.userParameters = userParameters;
    this.evalNum = evalNum;
    this.evalSize = evalSize;
    this.inputDir = inputDir;
    this.onLocal = onLocal;
    this.timeout = timeout;
  }

  public final String getIdentifier() {
    return identifier;
  }

  public final Configuration getUserCtrlTaskConf() {
    Configuration ctrlTaskConf =  Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(UserControllerTask.class, userControllerTask.getClass())
        .build();
    return Configurations.merge(userParameters.getUserCmpTaskConf(), ctrlTaskConf);
  }

  public final Configuration getUserCmpTaskConf() {
    Configuration cmpTaskConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(UserComputeTask.class, userComputeTask.getClass())
        .build();
    return Configurations.merge(userParameters.getUserCtrlTaskConf(), cmpTaskConf);
  }

  public final Configuration getDriverConf() {
    Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(UserControllerTask.class, userControllerTask.getClass())
        .bindImplementation(UserComputeTask.class, userComputeTask.getClass())
        .build();
    return Configurations.merge(userParameters.getDriverConf(), driverConf);
  }

  public final int getEvalNum() {
    return evalNum;
  }

  public final int getEvalSize() {
    return evalSize;
  }

  public final String getInputDir() {
    return inputDir;
  }

  public final boolean getOnLocal() {
    return onLocal;
  }

  public final int getTimeout() {
    return timeout;
  }

}
