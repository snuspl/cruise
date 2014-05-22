package org.apache.reef.inmemory.fs;

import java.io.Serializable;
import java.util.List;

/**
 * Created by i1befree on 2014. 5. 10..
 */
public class BlockInfo implements Serializable{
    private long blockId;
    private int offSet;
    private int length;
    private List<String> locations;

    public long getBlockId() {
        return blockId;
    }

    public void setBlockId(long blockId) {
        this.blockId = blockId;
    }

    public int getOffSet() {
        return offSet;
    }

    public void setOffSet(int offSet) {
        this.offSet = offSet;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public List<String> getLocations() {
        return locations;
    }

    public void setLocations(List<String> locations) {
        this.locations = locations;
    }
}
