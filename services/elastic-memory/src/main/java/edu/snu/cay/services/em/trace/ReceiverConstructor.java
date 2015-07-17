package edu.snu.cay.services.em.trace;

import edu.snu.cay.services.em.trace.parameters.ReceiverHost;
import edu.snu.cay.services.em.trace.parameters.ReceiverPort;
import edu.snu.cay.services.em.trace.parameters.ReceiverType;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.impl.StandardOutSpanReceiver;
import org.apache.htrace.impl.ZipkinSpanReceiver;
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
      this.receiver = new StandardOutSpanReceiver(HTraceConfiguration.EMPTY);
    } else if (ZIPKIN.equals(receiverType)) {
      this.receiver = getZipkinReceiver(receiverHost, receiverPort);
    } else {
      throw new RuntimeException("Unknown receiverType "+receiverType);
    }
  }

  private static ZipkinSpanReceiver getZipkinReceiver(final String receiverHost, final int receiverPort) {
    final Map<String, String> confMap = new HashMap<>(2);
    confMap.put("zipkin.collector-hostname", receiverHost);
    confMap.put("zipkin.collector-port", Integer.toString(receiverPort));
    return new ZipkinSpanReceiver(HTraceConfiguration.fromMap(confMap));
  }

  @Override
  public SpanReceiver newInstance() {
    return receiver;
  }
}
