package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Local implements java.io.Serializable {
    private static final long serialVersionUID = -7025011222312385692L;
    
    Chord chord;
    Logger logger;
    Network network;
    User user;
    
    public Local(@JsonProperty("chord") Chord chord, 
                 @JsonProperty("logger") Logger logger,
                 @JsonProperty("network") Network network,
                 @JsonProperty("user") User user) {
        super();
        this.chord=chord;
        this.logger=logger;
        this.network=network;
        this.user=user;
    }
    
    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public Network getNetwork() {
        return network;
    }

    public void setNetwork(Network network) {
        this.network = network;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}
