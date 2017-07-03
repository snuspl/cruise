/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.jobserver;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;

/**
 * Client for closing running job server. This is called by {#close_jobserver.sh}
 */
public final class JobServerCloser {

  private JobServerCloser() {
  }

  public static void main(final String[] args) {
    try {
      final CommandLine cl = new CommandLine();
      cl.registerShortNameOfClass(Parameters.HttpAddress.class);
      cl.registerShortNameOfClass(Parameters.HttpPort.class);

      // http configuration, target of http request is specified by this configuration.
      final Configuration httpConf = cl.processCommandLine(args).getBuilder().build();
      final Injector httpParamInjector = Tang.Factory.getTang().newInjector(httpConf);
      final String address = httpParamInjector.getNamedInstance(Parameters.HttpAddress.class);
      final String port = httpParamInjector.getNamedInstance(Parameters.HttpPort.class);
      HttpSender.sendFinishCommand(address, port);

    } catch (IOException | InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
