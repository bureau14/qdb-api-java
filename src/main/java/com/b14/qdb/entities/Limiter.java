package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Limiter implements java.io.Serializable  {
    private static final long serialVersionUID = 7947626247907905742L;
    
    long max_bytes;
    long max_in_entries_count;
    
    public Limiter(@JsonProperty("max_bytes") long max_bytes, 
                   @JsonProperty("max_in_entries_count") long max_in_entries_count) {
        super();
        this.max_bytes=max_bytes;
        this.max_in_entries_count=max_in_entries_count;
    }

    public long getMax_bytes() {
        return max_bytes;
    }

    public void setMax_bytes(long max_bytes) {
        this.max_bytes = max_bytes;
    }

    public long getMax_in_entries_count() {
        return max_in_entries_count;
    }

    public void setMax_in_entries_count(long max_in_entries_count) {
        this.max_in_entries_count = max_in_entries_count;
    }
    
}
