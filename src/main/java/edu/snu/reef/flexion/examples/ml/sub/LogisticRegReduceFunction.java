package edu.snu.reef.flexion.examples.ml.sub;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.flexion.examples.ml.data.LogisticRegSummary;

import javax.inject.Inject;

public class LogisticRegReduceFunction implements Reduce.ReduceFunction<LogisticRegSummary> {

  @Inject
  public LogisticRegReduceFunction() {
  }

  @Override
  public final LogisticRegSummary apply(Iterable<LogisticRegSummary> summaryList) {
    LogisticRegSummary reducedSummary = null;
    for (final LogisticRegSummary summary : summaryList) {
      if (reducedSummary==null) {
        reducedSummary = summary;
      } else {
        reducedSummary.plus(summary);
      }
    }
    return reducedSummary;
  }
}