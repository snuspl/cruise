package edu.snu.reef.elastic.memory;

public final class ElasticMemoryCtrlMsg {

  private final String dataClassName;
  private final String destId;

  public ElasticMemoryCtrlMsg(final String dataClassName, final String destId) {
    this.dataClassName = dataClassName;
    this.destId = destId;
  }

  public final String getDataClassName() {
    return dataClassName;
  }

  public final String getDestId() {
    return destId;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(ElasticMemoryCtrlMsg.class.getSimpleName())
        .append("[send ")
        .append(dataClassName)
        .append(" to ")
        .append(destId)
        .append("]");
    return sb.toString();
  }
}
