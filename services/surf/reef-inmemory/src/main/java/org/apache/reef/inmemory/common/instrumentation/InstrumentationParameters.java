package org.apache.reef.inmemory.common.instrumentation;

import com.codahale.metrics.ScheduledReporter;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.util.Set;

public final class InstrumentationParameters {

  @NamedParameter(doc = "Reporters")
  public static final class InstrumentationReporters implements Name<Set<ExternalConstructor<ScheduledReporter>>> {
  }

  @NamedParameter(doc = "Reporting period in seconds", default_value = "60", short_name = "reporting_period")
  public static final class InstrumentationReporterPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "Reporting LOG Level", default_value = "FINE", short_name = "reporting_log_level")
  public static final class InstrumentationLogLevel implements Name<String> {
  }

}
