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
package edu.snu.cay.dolphin.bsp.core;

import edu.snu.cay.common.param.Parameters.*;
import edu.snu.cay.dolphin.bsp.groupcomm.conf.GroupCommParameters;
import edu.snu.cay.dolphin.bsp.parameters.OutputDir;
import edu.snu.cay.dolphin.bsp.parameters.StartTrace;
import edu.snu.cay.services.em.common.parameters.ElasticMemoryParameters;
import edu.snu.cay.services.em.optimizer.conf.OptimizerParameters;
import edu.snu.cay.services.em.plan.conf.PlanExecutorParameters;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;

import java.io.IOException;

public final class DolphinConfiguration extends ConfigurationModuleBuilder {
  public static Configuration getConfiguration(final String[] args) throws IOException {
    return getConfiguration(args, new CommandLine());
  }

  public static Configuration getConfiguration(final String[] args, final CommandLine cl) throws IOException {
    cl.registerShortNameOfClass(EvaluatorSize.class);
    cl.registerShortNameOfClass(NumEvaluatorCores.class);
    cl.registerShortNameOfClass(OnLocal.class);
    cl.registerShortNameOfClass(LocalRuntimeMaxNumEvaluators.class);
    cl.registerShortNameOfClass(Splits.class);
    cl.registerShortNameOfClass(Timeout.class);
    cl.registerShortNameOfClass(InputDir.class);
    cl.registerShortNameOfClass(OutputDir.class);
    cl.registerShortNameOfClass(StartTrace.class);
    HTraceParameters.registerShortNames(cl);
    OptimizerParameters.registerShortNames(cl);
    PlanExecutorParameters.registerShortNames(cl);
    GroupCommParameters.registerShortNames(cl);
    ElasticMemoryParameters.registerShortNames(cl);
    final ConfigurationBuilder cb = cl.getBuilder();
    cl.processCommandLine(args);
    return cb.build();
  }
}
