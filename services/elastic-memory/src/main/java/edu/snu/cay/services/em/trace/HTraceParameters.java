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
package edu.snu.cay.services.em.trace;

import edu.snu.cay.services.em.trace.parameters.ReceiverHost;
import edu.snu.cay.services.em.trace.parameters.ReceiverPort;
import edu.snu.cay.services.em.trace.parameters.ReceiverType;
import org.apache.htrace.SpanReceiver;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * A class holding HTrace parameters.
 * The configured parameters are used to create a SpanReceiver instance.
 */
public final class HTraceParameters {

  private final String receiver;
  private final String receiverHost;
  private final int receiverPort;

  @Inject
  private HTraceParameters(@Parameter(ReceiverType.class) final String receiver,
                           @Parameter(ReceiverHost.class) final String receiverHost,
                           @Parameter(ReceiverPort.class) final int receiverPort) {
    this.receiver = receiver;
    this.receiverHost = receiverHost;
    this.receiverPort = receiverPort;
  }

  /**
   * @return A fully-configured Tang Configuration given the instantiated HTraceParameters.
   *         This configuration should be passed from the Client to Driver, and from the Driver to Evaluators.
   */
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(HTraceParameters.class, HTraceParameters.class)
        .bindConstructor(SpanReceiver.class, ReceiverConstructor.class)
        .bindNamedParameter(ReceiverType.class, receiver)
        .bindNamedParameter(ReceiverHost.class, receiverHost)
        .bindNamedParameter(ReceiverPort.class, Integer.toString(receiverPort))
        .build();
  }

  /**
   * Register all short names to the command line parser, for use at the client.
   * @param commandLine The CommandLine instantiated at the client.
   * @return The CommandLine after short names are registered.
   */
  public static CommandLine registerShortNames(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(ReceiverType.class)
        .registerShortNameOfClass(ReceiverHost.class)
        .registerShortNameOfClass(ReceiverPort.class);
  }
}
