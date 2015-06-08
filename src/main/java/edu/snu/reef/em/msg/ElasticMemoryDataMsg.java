package edu.snu.reef.em.msg;

public final class ElasticMemoryDataMsg {
  private final String from;
  private final String to;
  private final String dataClassName;
  private final byte[][] data;

  public ElasticMemoryDataMsg(final String from,
                              final String to,
                              final String dataClassName,
                              final byte[][] data) {
    this.from = from;
    this.to = to;
    this.dataClassName = dataClassName;
    this.data = data;
  }

  public final String getFrom() {
    return from;
  }

  public final String getTo() {
    return to;
  }

  public final String getDataClassName() {
    return dataClassName;
  }

  public final byte[][] getData() {
    return data;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(ElasticMemoryDataMsg.class.getSimpleName())
        .append("[From ")
        .append(from)
        .append(" to ")
        .append(to)
        .append(": ")
        .append(dataClassName)
        .append("]");
    return sb.toString();
  }
}
