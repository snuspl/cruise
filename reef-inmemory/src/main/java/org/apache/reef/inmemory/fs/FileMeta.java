package org.apache.reef.inmemory.fs;

import java.io.Serializable;
import java.util.List;

/**
 * Surf File meta information
 */
public class FileMeta implements Serializable{
    private long fileId;
    private String fileName;
    private String fullPath;
    private long fileSize;
    private long blockSize;
    private long creationTime;
    private boolean directory;
    private List<BlockInfo> blocks;
    private boolean complete;
    private User owner;

    public FileMeta(){
        //creationTime = System.currentTimeMillis();
        complete = true;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFullPath() {
        return fullPath;
    }

    public void setFullPath(String fullPath) {
        this.fullPath = fullPath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public boolean isDirectory() {
        return directory;
    }

    public void setDirectory(boolean directory) {
        this.directory = directory;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public List<BlockInfo> getBlocks() {
        return blocks;
    }

    public void setBlocks(List<BlockInfo> blocks) {
        this.blocks = blocks;
    }

    public User getOwner() {
        return owner;
    }

    public void setOwner(User owner) {
        this.owner = owner;
    }
}
