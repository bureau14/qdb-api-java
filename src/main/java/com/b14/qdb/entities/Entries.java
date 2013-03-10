package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Entries implements java.io.Serializable {
    private static final long serialVersionUID = -2649767686268390331L;
    
    Entry persisted;
    Entry resident;

    public Entries(@JsonProperty("persisted") Entry persisted,
                   @JsonProperty("resident") Entry resident) {
        super();
        this.persisted=persisted;
        this.resident=resident;
    }

    public Entry getPersisted() {
        return persisted;
    }

    public void setPersisted(Entry persisted) {
        this.persisted = persisted;
    }

    public Entry getResident() {
        return resident;
    }

    public void setResident(Entry resident) {
        this.resident = resident;
    }

}
