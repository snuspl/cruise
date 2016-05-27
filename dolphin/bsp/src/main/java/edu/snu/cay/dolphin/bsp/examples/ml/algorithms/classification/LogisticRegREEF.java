/*
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
package edu.snu.cay.dolphin.bsp.examples.ml.algorithms.classification;

import edu.snu.cay.dolphin.bsp.core.DolphinLauncher;
import edu.snu.cay.dolphin.bsp.core.DolphinConfiguration;
import edu.snu.cay.dolphin.bsp.core.UserJobInfo;
import edu.snu.cay.dolphin.bsp.parameters.JobIdentifier;
import edu.snu.cay.dolphin.bsp.core.UserParameters;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

public final class LogisticRegREEF {
  /**
   * Should not be instantiated.
   */
  private LogisticRegREEF() {
  }

  public static void main(final String[] args) throws Exception {
    DolphinLauncher.run(
        Configurations.merge(
            DolphinConfiguration.getConfiguration(args, LogisticRegParameters.getCommandLine()),
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(JobIdentifier.class, "Logistic Regression")
                .bindImplementation(UserJobInfo.class, LogisticRegJobInfo.class)
                .bindImplementation(UserParameters.class, LogisticRegParameters.class)
                .build()
        )
    );
  }
}
