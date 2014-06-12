package org.apache.reef.inmemory.cache;

public class FileBlock {
  private final String path;
  private final long offset;
  private final long length;

  FileBlock(final String path, final long offset, final long length) {
    this.path = path;
    this.offset = offset;
    this.length = length;
  }

  public String getPath() {
    return path;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FileBlock fileBlock = (FileBlock) o;

    if (offset != fileBlock.offset) return false;
    if (!path.equals(fileBlock.path)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    return result;
  }
}
