package edu.snu.reef.elastic.memory;

public final class ElasticMemoryMessage {
  private final String from;
  private final String to;
  private final Type msgType;
  private final String dataClassName;
  private final byte[] data;

  public ElasticMemoryMessage(final String from,
                              final String to,
                              final Type msgType,
                              final String dataClassName,
                              final byte[] data) {
    this.from = from;
    this.to = to;
    this.msgType = msgType;
    this.dataClassName = dataClassName;
    this.data = data;
  }

  public final String getFrom() {
    return from;
  }

  public final String getTo() {
    return to;
  }

  public final Type getMsgType() {
    return msgType;
  }

  public final String getDataClassName() {
    return dataClassName;
  }

  public final byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(ElasticMemoryMessage.class.getSimpleName())
        .append("[")
        .append(msgType)
        .append(" from ")
        .append(from)
        .append(" to ")
        .append(to)
        .append(": ")
        .append(dataClassName)
        .append("]");
    return sb.toString();
  }

  public enum Type {
    CTRL, DATA;

    public static int toInt(Type type) {
      return type == CTRL ? 0 : 1;
    }

    public static Type toType(int i) {
      return i == 0 ? CTRL : DATA;
    }
  }
}
