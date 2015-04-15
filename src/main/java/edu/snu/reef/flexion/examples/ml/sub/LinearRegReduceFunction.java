package edu.snu.reef.flexion.examples.ml.sub;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.flexion.examples.ml.data.LinearRegSummary;

import javax.inject.Inject;

public class LinearRegReduceFunction implements Reduce.ReduceFunction<LinearRegSummary> {

  @Inject
  public LinearRegReduceFunction() {
  }

  @Override
  public final LinearRegSummary apply(Iterable<LinearRegSummary> summaryList) {
    LinearRegSummary reducedSummary = null;
    for (final LinearRegSummary summary : summaryList) {
      if (reducedSummary==null) {
        reducedSummary = summary;
      } else {
        reducedSummary.plus(summary);
      }
    }
    return reducedSummary;
  }
}