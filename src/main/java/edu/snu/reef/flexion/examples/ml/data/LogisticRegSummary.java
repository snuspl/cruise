package edu.snu.reef.flexion.examples.ml.data;


public class LogisticRegSummary {

  private final LinearModel model;
  private int count;
  private int posNum;
  private int negNum;

  public LogisticRegSummary(LinearModel model, int count, int posNum, int negNum) {
    this.model = model;
    this.count = count;
    this.posNum = posNum;
    this.negNum = negNum;
  }

  public void plus(LogisticRegSummary summary) {
    this.model.setParameters(this.model.getParameters().plus(summary.getModel().getParameters()));
    this.count += summary.count;
    this.posNum += summary.posNum;
    this.negNum += summary.negNum;
  }

  public LinearModel getModel() {
    return model;
  }

  public int getCount() {
    return count;
  }

  public int getPosNum() {
    return posNum;
  }

  public int getNegNum() {
    return negNum;
  }

}
