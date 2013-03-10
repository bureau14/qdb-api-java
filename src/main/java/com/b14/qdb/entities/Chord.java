package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Chord implements java.io.Serializable {
    private static final long serialVersionUID = 3711325082489249311L;
    
    int[] bootstrapping_peers;
    boolean no_stabilization;
    String node_id;

    public Chord(@JsonProperty("bootstrapping_peers") int[] bootstrapping_peers, 
                 @JsonProperty("no_stabilization") boolean no_stabilization, 
                 @JsonProperty("node_id") String node_id) {
        super();
        this.bootstrapping_peers=bootstrapping_peers;
        this.no_stabilization=no_stabilization;
        this.node_id=node_id;
    }

    public int[] getBootstrapping_peers() {
        return bootstrapping_peers;
    }

    public void setBootstrapping_peers(int[] bootstrapping_peers) {
        this.bootstrapping_peers = bootstrapping_peers;
    }

    public boolean isNo_stabilization() {
        return no_stabilization;
    }

    public void setNo_stabilization(boolean no_stabilization) {
        this.no_stabilization = no_stabilization;
    }

    public String getNode_id() {
        return node_id;
    }

    public void setNode_id(String node_id) {
        this.node_id = node_id;
    }

}
