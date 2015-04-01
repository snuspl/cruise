package org.apache.reef.inmemory.driver.metatree;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class DirectoryEntry implements Entry {
  private String name;
  private DirectoryEntry parent;
  final List<Entry> children = new ArrayList<>(1);

  /**
   * @param name The name of the directory
   * @param parent The parent directory (For the root directory, it is null)
   */
  @Inject
  public DirectoryEntry(final String name,
                        final DirectoryEntry parent) {
    this.name = name;
    this.parent = parent;
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
  public void rename(final Entry dstEntry) {
    this.parent.removeChild(this);
    this.name = dstEntry.getName();
    this.parent = dstEntry.getParent();
    this.parent.addChild(this);
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
    return true;
  }

  public List<Entry> getChildren() {return children;}

  public void addChild(final Entry entry) {
    children.add(entry);
  }

  public boolean removeChild(final Entry entry) {
    return children.remove(entry);
  }

  public void removeAllChildren() {
    children.clear();
  }
}