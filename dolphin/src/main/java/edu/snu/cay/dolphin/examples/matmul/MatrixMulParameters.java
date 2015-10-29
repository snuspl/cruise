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
package edu.snu.cay.dolphin.examples.matmul;

import edu.snu.cay.dolphin.core.UserParameters;
import org.apache.reef.driver.parameters.JobGlobalFiles;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;
import java.io.File;

public final class MatrixMulParameters implements UserParameters {

  private final String filePath;

  @Inject
  private MatrixMulParameters(@Parameter(SmallMatrixFilePath.class) final String filePath) {
    this.filePath = filePath;
  }

  @Override
  public Configuration getDriverConf() {
    final File localFile = new File(filePath);
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(JobGlobalFiles.class, localFile.getAbsolutePath())
        .bindNamedParameter(SmallMatrixFilePath.class, "reef/global/" + localFile.getName())
        .build();
  }

  @Override
  public Configuration getServiceConf() {
    return Tang.Factory.getTang().newConfigurationBuilder().build();
  }

  @Override
  public Configuration getUserCmpTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(SmallMatrixFilePath.class, filePath)
        .build();
  }

  @Override
  public Configuration getUserCtrlTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder().build();
  }

  public static CommandLine getCommandLine() {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(SmallMatrixFilePath.class);
    return cl;
  }
}
