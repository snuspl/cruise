package org.apache.reef.inmemory.common.instrumentation;

import com.codahale.metrics.ScheduledReporter;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

import java.util.Set;

public final class InstrumentationParameters {

  @NamedParameter(doc = "Reporters")
  public final static class InstrumentationReporters implements Name<Set<ExternalConstructor<ScheduledReporter>>> {
  }

  @NamedParameter(doc = "Reporting period in seconds", default_value = "60", short_name = "reporting_period")
  public final static class InstrumentationReporterPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "Reporting LOG Level", default_value = "FINE", short_name = "reporting_log_level")
  public final static class InstrumentationLogLevel implements Name<String> {
  }

}
