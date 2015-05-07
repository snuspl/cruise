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

import org.apache.hadoop.fs.Path;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Unit;
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
   * Output writer provides a user interface through which tasks write their output
   */
  private final OutputWriter outputWriter;

  @Inject
  private OutputService(OutputWriter outputWriter) {
    this.outputWriter = outputWriter;
  }

  /**
   * Provides a configuration for Output service
   * @param evaluatorId
   * @param outputDir
   * @param onLocal
   * @return
   */
  public static Configuration getServiceConfiguration(
      final String evaluatorId, final String outputDir, final boolean onLocal) {
    Class<? extends OutputWriter> outputWriterClass = onLocal ? OutputWriterLocal.class : OutputWriterHDFS.class;

    Configuration partialServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, outputWriterClass)
        .set(ServiceConfiguration.ON_CONTEXT_STARTED, ContextStartHandler.class)
        .set(ServiceConfiguration.ON_CONTEXT_STOP, ContextStopHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(partialServiceConf)
        .bindImplementation(OutputWriter.class, outputWriterClass)
        .bindNamedParameter(OutputPath.class, getFullOutputPath(outputDir, evaluatorId))
        .build();
  }

  /**
   * Return an output file path which are the concatenation of
   * the output directory path given by the user and the id of the current evaluator
   * @param outputDir output directory path given by the user
   * @param evaluatorID id of the current evaluator
   * @return
   */
  private static String getFullOutputPath(final String outputDir, final String evaluatorID) {
    return outputDir + Path.SEPARATOR + evaluatorID;
  }

  private final class ContextStartHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(ContextStart contextStart) {
      LOG.log(Level.INFO, "Context started, create outputWriter.");
      try {
        outputWriter.create();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private final class ContextStopHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(ContextStop contextStop) {
      LOG.log(Level.INFO, "Context stopped, close outputWriter.");
      try {
        outputWriter.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @NamedParameter(doc = "File to write output data to")
  final class OutputPath implements Name<String> {
  }
}