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
package edu.snu.reef.flexion.examples.ml.algorithms.classification;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.parameters.JobIdentifier;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

public class LogisticRegREEF {
  public final static void main(String[] args) throws Exception {
    FlexionLauncher.run(
        Configurations.merge(
            FlexionConfiguration.CONF(args, LogisticRegParameters.getCommandLine()),
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(JobIdentifier.class, "Logistic Regression")
                .bindImplementation(UserJobInfo.class, LogisticRegJobInfo.class)
                .bindImplementation(UserParameters.class, LogisticRegParameters.class)
                .build()
        )
    );
  }
}
