package org.apache.reef.inmemory.fs;

import java.io.Serializable;

/**
 * User information class
 */
public class User implements Serializable {
  private String id;
  private String group;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }
}
