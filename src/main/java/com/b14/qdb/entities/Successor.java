package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Successor implements java.io.Serializable {
    private static final long serialVersionUID = -2649767686268390331L;
    
    String endpoint;
    String reference;

    public Successor(@JsonProperty("endpoint") String endpoint,
                       @JsonProperty("reference") String reference) {
        super();
        this.endpoint=endpoint;
        this.reference=reference;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }
    
}
