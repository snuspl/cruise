package org.apache.reef.inmemory.fs.commands;

import org.apache.reef.inmemory.fs.User;

import java.io.Serializable;

/**
 * List of directories or files command
 */
public class ListCommand implements Serializable {
    private String path;
    private boolean recursive;
    private User user;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}
