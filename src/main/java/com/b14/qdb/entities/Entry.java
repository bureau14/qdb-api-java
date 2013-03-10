package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Entry implements java.io.Serializable {
    private static final long serialVersionUID = -2649767686268390331L;
    
    long count;
    long size;

    public Entry(@JsonProperty("count") long count,
                  @JsonProperty("size") long size) {
        super();
        this.count=count;
        this.size=size;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
    
}
