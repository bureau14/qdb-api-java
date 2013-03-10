package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class NodeTopology implements java.io.Serializable {
    private static final long serialVersionUID = -6066263008358013057L;
    
    Center center;
    Predecessor predecessor;
    Successor successor;
    
    public NodeTopology(@JsonProperty("center") Center center,
                        @JsonProperty("predecessor") Predecessor predecessor,
                        @JsonProperty("successor") Successor successor) {
        super();
        this.center=center;
        this.predecessor=predecessor;
        this.successor=successor;
    }

    public Center getCenter() {
        return center;
    }

    public void setCenter(Center center) {
        this.center = center;
    }

    public Predecessor getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(Predecessor predecessor) {
        this.predecessor = predecessor;
    }

    public Successor getSuccessor() {
        return successor;
    }

    public void setSuccessor(Successor successor) {
        this.successor = successor;
    }
    
}
