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
package edu.snu.cay.dolphin.core;

import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A service class of Output service.
 * Output service provides an Output writer,
 * through which tasks write their output to the file given by the user
 */
@Unit
public final class OutputService {
  private static Logger LOG = Logger.getLogger(OutputService.class.getName());

  /**
   * A provider through which users create output streams
   */
  private final OutputStreamProvider outputStreamProvider;

  @Inject
  private OutputService(OutputStreamProvider outputStreamProvider) {
    this.outputStreamProvider = outputStreamProvider;
  }

  /**
   * Provides a configuration for Output service
   * @param outputDir
   * @param onLocal
   * @return
   */
  public static Configuration getServiceConfiguration(final String outputDir, final boolean onLocal) {
    Class<? extends OutputStreamProvider> outputStreamProviderClass
        = onLocal ? OutputStreamProviderLocal.class : OutputStreamProviderHDFS.class;

    Configuration partialServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, outputStreamProviderClass)
        .set(ServiceConfiguration.ON_CONTEXT_STOP, ContextStopHandler.class)
        .set(ServiceConfiguration.ON_TASK_STARTED, TaskStartHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(partialServiceConf)
        .bindImplementation(OutputStreamProvider.class, outputStreamProviderClass)
        .bindNamedParameter(OutputPath.class, outputDir)
        .build();
  }

  private final class ContextStopHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(ContextStop contextStop) {
      LOG.log(Level.INFO, "Context stopped, close the OutputStreamProvider.");
      try {
        outputStreamProvider.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final class TaskStartHandler implements EventHandler<TaskStart> {
    @Override
    public void onNext(TaskStart taskStart) {
      LOG.log(Level.INFO, String.format("Task %s started, create the OutputStreamProvider.", taskStart.getId()));
      outputStreamProvider.setTaskId(taskStart.getId());
    }
  }

  @NamedParameter(doc = "Path of the directory to write output data to")
  final class OutputPath implements Name<String> {
  }
}