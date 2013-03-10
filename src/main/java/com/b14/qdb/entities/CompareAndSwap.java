package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class CompareAndSwap implements java.io.Serializable {
    private static final long serialVersionUID = -582422130657648393L;
    
    long count;
    long evictions;
    long failures;
    long pageins;
    long size;
    long successes;
    
    public CompareAndSwap(@JsonProperty("count") long count,
               @JsonProperty("evictions") long evictions,
               @JsonProperty("failures") long failures,
               @JsonProperty("pageins") long pageins,
               @JsonProperty("size") long size,
               @JsonProperty("successes") long successes) {
        super();
        this.count=count;
        this.evictions=evictions;
        this.failures=failures;
        this.pageins=pageins;
        this.size=size;
        this.successes=successes;
    }
    
    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getEvictions() {
        return evictions;
    }

    public void setEvictions(long evictions) {
        this.evictions = evictions;
    }

    public long getFailures() {
        return failures;
    }

    public void setFailures(long failures) {
        this.failures = failures;
    }

    public long getPageins() {
        return pageins;
    }

    public void setPageins(long pageins) {
        this.pageins = pageins;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getSuccesses() {
        return successes;
    }

    public void setSuccesses(long successes) {
        this.successes = successes;
    }
}
