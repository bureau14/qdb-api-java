package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Memory implements java.io.Serializable {
    private static final long serialVersionUID = -2649767686268390331L;
    
    long total;
    long used;

    public Memory(@JsonProperty("total") long total,
                  @JsonProperty("used") long used) {
        super();
        this.total=total;
        this.used=used;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getUsed() {
        return used;
    }

    public void setUsed(long used) {
        this.used = used;
    }
    
}
