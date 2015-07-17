package edu.snu.cay.services.em.trace;

import org.apache.htrace.Sampler;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import javax.inject.Inject;

/**
 * This class ensures that HTrace is wired up on the process.
 * The EM implementation instantiates it via Tang at each process.
 * User code needs only to call Trace.startSpan(String, Sampler) to start a trace.
 */
public final class HTrace {

  @Inject
  private HTrace(final SpanReceiver spanReceiver) {
    Trace.addReceiver(spanReceiver);
    initialTrace();
  }

  /**
   * We've noticed a ~200ms delay when calling startSpan for the first time.
   * Calling initialTrace here moves this delay from trace-time to Tang construction time.
   * The reason for the delay is conjectured to be the lazy initialization at {@link org.apache.htrace.Tracer}
   */
  private void initialTrace() {
    final TraceScope traceScope = Trace.startSpan("initialTrace", Sampler.ALWAYS);
    traceScope.close();
  }

  /**
   * Initialize HTrace.
   */
  public final void initialize() {
    // Left empty, because the constructor does the initialization.
    // The method is here as a reminder that an instance of this class must be
    // injected before using Trace.[static method] calls.
  }

}
