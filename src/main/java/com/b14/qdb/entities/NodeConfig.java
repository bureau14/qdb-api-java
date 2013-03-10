package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class NodeConfig implements java.io.Serializable {
    private static final long serialVersionUID = -6066263008358013057L;
    
    Global global;
    Local local;
    
    public NodeConfig(@JsonProperty("global") Global global,
                      @JsonProperty("local") Local local) {
        super();
        this.global=global;
        this.local=local;
    }
    
    public Global getGlobal() {
        return global;
    }

    public void setGlobal(Global global) {
        this.global = global;
    }

    public Local getLocal() {
        return local;
    }

    public void setLocal(Local local) {
        this.local = local;
    }
}
