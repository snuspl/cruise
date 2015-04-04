package org.apache.reef.inmemory.driver.metatree;

import org.apache.reef.inmemory.common.entity.FileMeta;

import javax.inject.Inject;

public class FileEntry implements Entry {
  private String name;
  private DirectoryEntry parent;
  final private FileMeta fileMeta;

  @Inject
  public FileEntry(final String name,
                   final DirectoryEntry parent,
                   final FileMeta fileMeta) {
    this.name = name;
    this.parent = parent;
    this.fileMeta = fileMeta;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DirectoryEntry getParent() {
    return parent;
  }

  @Override
  public void rename(final String dstFileName, final DirectoryEntry dstParent) {
    this.parent.removeChild(this);
    this.name = dstFileName;
    this.parent = dstParent;
    this.parent.addChild(this);
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  public FileMeta getFileMeta() {
    return fileMeta;
  }
}
