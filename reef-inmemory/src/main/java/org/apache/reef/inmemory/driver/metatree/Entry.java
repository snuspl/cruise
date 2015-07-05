package org.apache.reef.inmemory.driver.metatree;

public interface Entry {
  String getName();

  DirectoryEntry getParent();

  void rename(final String dstFileName, final DirectoryEntry dstParent);

  boolean isDirectory();
}
