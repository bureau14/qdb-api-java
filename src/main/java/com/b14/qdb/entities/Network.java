package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Network implements java.io.Serializable {
    private static final long serialVersionUID = -2378784169608182063L;
    
    int client_timeout;
    int idle_timeout;
    String listen_on;
    int partitions_count;
    int server_sessions;
    
    public Network(@JsonProperty("client_timeout") int client_timeout,
                   @JsonProperty("idle_timeout") int idle_timeout,
                   @JsonProperty("listen_on") String listen_on,
                   @JsonProperty("partitions_count") int partitions_count,
                   @JsonProperty("server_sessions") int server_sessions) {
        super();
        this.client_timeout=client_timeout;
        this.idle_timeout=idle_timeout;
        this.listen_on=listen_on;
        this.partitions_count=partitions_count;
        this.server_sessions=server_sessions;
    }
    
    public int getClient_timeout() {
        return client_timeout;
    }

    public void setClient_timeout(int client_timeout) {
        this.client_timeout = client_timeout;
    }

    public int getIdle_timeout() {
        return idle_timeout;
    }

    public void setIdle_timeout(int idle_timeout) {
        this.idle_timeout = idle_timeout;
    }

    public String getListen_on() {
        return listen_on;
    }

    public void setListen_on(String listen_on) {
        this.listen_on = listen_on;
    }

    public int getPartitions_count() {
        return partitions_count;
    }

    public void setPartitions_count(int partitions_count) {
        this.partitions_count = partitions_count;
    }

    public int getServer_sessions() {
        return server_sessions;
    }

    public void setServer_sessions(int server_sessions) {
        this.server_sessions = server_sessions;
    }

}
