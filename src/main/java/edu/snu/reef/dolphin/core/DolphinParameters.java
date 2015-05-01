/**
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.reef.dolphin.core;

import edu.snu.reef.dolphin.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

public final class DolphinParameters {
  private final String identifier;
  private final UserJobInfo userJobInfo;
  private final UserParameters userParameters;
  private final int evalNum;
  private final int evalSize;
  private final String inputDir;
  private final boolean onLocal;
  private final int timeout;

  @Inject
  private DolphinParameters(@Parameter(JobIdentifier.class) final String identifier,
                            final UserJobInfo userJobInfo,
                            final UserParameters userParameters,
                            @Parameter(EvaluatorNum.class) final int evalNum,
                            @Parameter(EvaluatorSize.class) final int evalSize,
                            @Parameter(InputDir.class) final String inputDir,
                            @Parameter(OnLocal.class) final boolean onLocal,
                            @Parameter(Timeout.class) final int timeout) {
    this.identifier = identifier;
    this.userJobInfo = userJobInfo;
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

  public final Configuration getDriverConf() {
    Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(EvaluatorNum.class, String.valueOf(evalNum))
        .bindImplementation(UserJobInfo.class, userJobInfo.getClass())
        .bindImplementation(UserParameters.class, userParameters.getClass())
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
