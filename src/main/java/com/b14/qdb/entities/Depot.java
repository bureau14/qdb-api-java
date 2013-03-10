package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Depot implements java.io.Serializable {
    private static final long serialVersionUID = 1293030580206083714L;
    
    int replication_factor;
    String root;
    boolean sync;
    @JsonProperty("transient") boolean trans;
    
    public Depot(@JsonProperty("replication_factor") int replication_factor, 
                @JsonProperty("root") String root, 
                @JsonProperty("sync") boolean sync, 
                @JsonProperty("transient") boolean trans) {
        super();
        this.replication_factor=replication_factor;
        this.root=root;
        this.sync=sync;
        this.trans=trans;
    }

    public int getReplication_factor() {
        return replication_factor;
    }

    public void setReplication_factor(int replication_factor) {
        this.replication_factor = replication_factor;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public boolean isTrans() {
        return trans;
    }

    public void setTrans(boolean trans) {
        this.trans = trans;
    }
    
    
}
