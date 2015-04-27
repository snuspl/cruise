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
package edu.snu.reef.flexion.core;

import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple parse class given in the form of a service.
 * Should be inserted alongside a context.
 *
 * This class doesn't imply HOW data should be parsed; it just provides a
 * small interface for retrieving input data.
 *
 * The actual parse function needs to be given as an argument.
 */
@Unit
public final class DataParseService {
  private static Logger LOG = Logger.getLogger(DataParseService.class.getName());

  /**
   * parse function to exploit
   */
  private final DataParser dataParser;

  /**
   * This class is instantiated by TANG
   *
   * Constructor for parse manager, which accepts an actual parse function as a parameter
   * @param dataParser parse function to exploit
   */
  @Inject
  private DataParseService(DataParser dataParser) {
    this.dataParser = dataParser;
  }

  public static Configuration getServiceConfiguration(Class<? extends DataParser> dataParseClass) {
    Configuration partialServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, dataParseClass)
        .set(ServiceConfiguration.ON_CONTEXT_STARTED, ContextStartHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(partialServiceConf)
        .bindImplementation(DataParser.class, dataParseClass)
        .build();
  }

  private final class ContextStartHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(ContextStart contextStart) {
      LOG.log(Level.INFO, "Context started, asking parser to parse.");
      dataParser.parse();
    }
  }
}
