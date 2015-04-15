package edu.snu.reef.flexion.examples.ml.data;


public class LogisticRegSummary {

  private final LinearModel model;
  private int count = 0;
  private double loss = 0;

  public LogisticRegSummary(LinearModel model, int count, double loss) {
    this.model = model;
    this.count = count;
    this.loss = loss;
  }

  public void plus(LogisticRegSummary summary) {
    this.model.setParameters(this.model.getParameters().plus(summary.getModel().getParameters()));
    this.count += summary.count;
    this.loss += summary.loss;
  }

  public LinearModel getModel() {
    return model;
  }

  public int getCount() {
    return count;
  }

  public double getLoss() {
    return loss;
  }

}
