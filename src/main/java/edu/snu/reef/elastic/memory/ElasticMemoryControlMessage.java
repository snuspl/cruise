package edu.snu.reef.elastic.memory;

public final class ElasticMemoryControlMessage {
  private final String str;

  public ElasticMemoryControlMessage(final String str) {
    this.str = str;
  }

  public final String getString() {
    return str;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(ElasticMemoryControlMessage.class.getSimpleName())
        .append("[")
        .append(str)
        .append("]");
    return sb.toString();
  }
}
