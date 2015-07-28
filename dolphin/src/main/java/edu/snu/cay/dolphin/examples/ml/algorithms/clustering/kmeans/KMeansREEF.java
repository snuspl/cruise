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
package edu.snu.cay.dolphin.examples.ml.algorithms.clustering.kmeans;

import edu.snu.cay.dolphin.core.DolphinConfiguration;
import edu.snu.cay.dolphin.core.DolphinLauncher;
import edu.snu.cay.dolphin.core.UserJobInfo;
import edu.snu.cay.dolphin.core.UserParameters;
import edu.snu.cay.dolphin.parameters.JobIdentifier;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

public class KMeansREEF {
  public final static void main(String[] args) throws Exception {
    DolphinLauncher.run(
        Configurations.merge(
            DolphinConfiguration.CONF(args, KMeansParameters.getCommandLine()),
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(JobIdentifier.class, "K-means Clustering")
                .bindImplementation(UserJobInfo.class, KMeansJobInfo.class)
                .bindImplementation(UserParameters.class, KMeansParameters.class)
                .build()
        )
    );
  }
}
