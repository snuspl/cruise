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
package edu.snu.cay.dolphin.core;

import edu.snu.cay.dolphin.parameters.*;
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
    cl.registerShortNameOfClass(OnLocal.class);
    cl.registerShortNameOfClass(LocalRuntimeMaxNumEvaluators.class);
    cl.registerShortNameOfClass(DesiredSplits.class);
    cl.registerShortNameOfClass(Timeout.class);
    cl.registerShortNameOfClass(InputDir.class);
    cl.registerShortNameOfClass(OutputDir.class);
    final ConfigurationBuilder cb = cl.getBuilder();
    cl.processCommandLine(args);
    return cb.build();
  }
}
