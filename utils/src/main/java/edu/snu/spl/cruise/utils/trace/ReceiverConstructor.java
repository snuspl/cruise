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
package edu.snu.spl.cruise.utils.trace;

import edu.snu.spl.cruise.utils.trace.parameters.ReceiverHost;
import edu.snu.spl.cruise.utils.trace.parameters.ReceiverPort;
import edu.snu.spl.cruise.utils.trace.parameters.ReceiverType;
import org.htrace.HTraceConfiguration;
import org.htrace.SpanReceiver;
import org.htrace.impl.StandardOutSpanReceiver;
import org.htrace.impl.ZipkinSpanReceiver;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * A constructor that can create one of StandardOut or Zipkin receivers.
 */
final class ReceiverConstructor implements ExternalConstructor<SpanReceiver> {

  public static final String ZIPKIN = "ZIPKIN";
  public static final String STDOUT = "STDOUT";

  private final SpanReceiver receiver;

  @Inject
  private ReceiverConstructor(@Parameter(ReceiverType.class) final String receiverType,
                              @Parameter(ReceiverHost.class) final String receiverHost,
                              @Parameter(ReceiverPort.class) final int receiverPort) {
    if (STDOUT.equals(receiverType)) {
      this.receiver = new StandardOutSpanReceiver();
    } else if (ZIPKIN.equals(receiverType)) {
      this.receiver = getZipkinReceiver(receiverHost, receiverPort);
    } else {
      throw new RuntimeException("Unknown receiverType " + receiverType);
    }
  }

  private static ZipkinSpanReceiver getZipkinReceiver(final String receiverHost, final int receiverPort) {
    final Map<String, String> confMap = new HashMap<>(2);
    confMap.put("zipkin.collector-hostname", receiverHost);
    confMap.put("zipkin.collector-port", Integer.toString(receiverPort));

    final ZipkinSpanReceiver receiver = new ZipkinSpanReceiver();
    receiver.configure(HTraceConfiguration.fromMap(confMap));
    return receiver;
  }

  @Override
  public SpanReceiver newInstance() {
    return receiver;
  }
}
