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
public class NodeStatus implements java.io.Serializable {
    private static final long serialVersionUID = -6066263008358013057L;
    
    String engine_build_date;
    String engine_version;
    Entries entries;
    int hardware_concurrency;
    String[] listening_addresses;
    Memories memory;
    String node_id;
    String operating_system;
    Operations operations;
    Overall overall;
    int partitions_count;
    String startup;
    String timestamp;

    public NodeStatus(@JsonProperty("engine_build_date") String engine_build_date,
                      @JsonProperty("engine_version") String engine_version,
                      @JsonProperty("entries") Entries entries,
                      @JsonProperty("hardware_concurrency") int hardware_concurrency,
                      @JsonProperty("listening_addresses") String[] listening_addresses,
                      @JsonProperty("memory") Memories memory,
                      @JsonProperty("node_id") String node_id,
                      @JsonProperty("operating_system") String operating_system,
                      @JsonProperty("operations") Operations operations,
                      @JsonProperty("overall") Overall overall,
                      @JsonProperty("partitions_count") int partitions_count,
                      @JsonProperty("startup") String startup,
                      @JsonProperty("timestamp") String timestamp) {
        super();
        this.engine_build_date=engine_build_date;
        this.engine_version=engine_version;
        this.entries=entries;
        this.hardware_concurrency=hardware_concurrency;
        this.listening_addresses=listening_addresses;
        this.memory=memory;
        this.node_id=node_id;
        this.operating_system=operating_system;
        this.operations=operations;
        this.overall=overall;
        this.partitions_count=partitions_count;
        this.startup=startup;
        this.timestamp=timestamp;
    }

    public String getEngine_build_date() {
        return engine_build_date;
    }

    public void setEngine_build_date(String engine_build_date) {
        this.engine_build_date = engine_build_date;
    }

    public String getEngine_version() {
        return engine_version;
    }

    public void setEngine_version(String engine_version) {
        this.engine_version = engine_version;
    }

    public Entries getEntries() {
        return entries;
    }

    public void setEntries(Entries entries) {
        this.entries = entries;
    }

    public int getHardware_concurrency() {
        return hardware_concurrency;
    }

    public void setHardware_concurrency(int hardware_concurrency) {
        this.hardware_concurrency = hardware_concurrency;
    }

    public String[] getListening_addresses() {
        return listening_addresses;
    }

    public void setListening_addresses(String[] listening_addresses) {
        this.listening_addresses = listening_addresses;
    }

    public Memories getMemory() {
        return memory;
    }

    public void setMemory(Memories memories) {
        this.memory = memories;
    }

    public String getNode_id() {
        return node_id;
    }

    public void setNode_id(String node_id) {
        this.node_id = node_id;
    }

    public String getOperating_system() {
        return operating_system;
    }

    public void setOperating_system(String operating_system) {
        this.operating_system = operating_system;
    }

    public Operations getOperations() {
        return operations;
    }

    public void setOperations(Operations operations) {
        this.operations = operations;
    }

    public Overall getOverall() {
        return overall;
    }

    public void setOverall(Overall overall) {
        this.overall = overall;
    }

    public int getPartitions_count() {
        return partitions_count;
    }

    public void setPartitions_count(int partitions_count) {
        this.partitions_count = partitions_count;
    }

    public String getStartup() {
        return startup;
    }

    public void setStartup(String startup) {
        this.startup = startup;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
}
