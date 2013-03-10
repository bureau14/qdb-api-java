package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class User implements java.io.Serializable {
    private static final long serialVersionUID = -2649767686268390331L;
    
    String license_file;

    public User(@JsonProperty("client_timeout") String license_file) {
        super();
        this.license_file=license_file;
    }
    
    public String getLicense_file() {
        return license_file;
    }

    public void setLicense_file(String license_file) {
        this.license_file = license_file;
    }
}
