package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Global implements java.io.Serializable {
    private static final long serialVersionUID = 486689501182273625L;
    
    Depot depot;
    Limiter limiter;
        
    public Global(@JsonProperty("depot") Depot depot, 
                  @JsonProperty("limiter") Limiter limiter) {
        super();
        this.depot=depot;
        this.limiter=limiter;
    }
    
    public Depot getDepot() {
        return depot;
    }

    public void setDepot(Depot depot) {
        this.depot = depot;
    }

    public Limiter getLimiter() {
        return limiter;
    }

    public void setLimiter(Limiter limiter) {
        this.limiter = limiter;
    }

}
