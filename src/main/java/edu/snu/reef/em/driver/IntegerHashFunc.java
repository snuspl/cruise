package edu.snu.reef.elastic.memory.driver;

public class IntegerHashFunc implements HashFunc<Integer> {
  private final int maxValue;

  public IntegerHashFunc(int maxValue) {
    this.maxValue = maxValue;
  }

  @Override
  public int hash(Integer key) {
    return key.intValue() % maxValue;
  }
}
