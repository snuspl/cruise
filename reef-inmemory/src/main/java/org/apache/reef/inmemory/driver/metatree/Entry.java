package org.apache.reef.inmemory.driver.metatree;

public interface Entry {
  public String getName();

  public DirectoryEntry getParent();

  public void rename(final Entry dstEntry);

  public void rename(final String dstFileName, final DirectoryEntry dstParent);

  public boolean isDirectory();
}
