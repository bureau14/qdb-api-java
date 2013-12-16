/**
 * Copyright (c) 2009-2013, Bureau 14 SARL
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of Bureau 14 nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

/**
 * Entity for supervision metrics.
 *
 * @author &copy; <a href="https://www.bureau14.fr/">bureau14</a> - 2013
 * @version master
 * @since 0.7.5
 */
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
