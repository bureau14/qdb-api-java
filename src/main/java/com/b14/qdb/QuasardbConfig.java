/**
 * Copyright (c) 2009-2015, quasardb SAS
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
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
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
package com.b14.qdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A convenient API configuration object.
 * <br>
 * <br>
 * You can provide the following elements to the API :
 * <ul>
 *      <li><u>Nodes :</u> nodes are a collection of {@link QuasardbNode}. Each node is a part of the quasardb cluster to contact.</li>
 *      <li><u>Expiry time :</u> default value for expiry time for all incoming data.</li>
 * </ul>
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.5
 */
public class QuasardbConfig {
    // Expiry time in seconds
    private long expiryTimeInSeconds = 0;
    
    // List of all nodes of the cluster
    private List<QuasardbNode> nodes = new ArrayList<QuasardbNode>();
    
    /**
     * Get default expiry time for all entries. Result is given in seconds.
     * 
     * @return expiry time in seconds.
     * @since 1.1.5
     */
    public long getExpiryTimeInSeconds() {
        return expiryTimeInSeconds;
    }

    /**
     * Set default expiry time for all new incoming entries of the cluster.
     * <br>
     * <b>Be careful :</b> unit for this parameter is second.
     *       
     * @param expiryTimeInSeconds expiry time in seconds for all new data.
     * @since 1.1.5
     */
    public void setExpiryTimeInSeconds(long expiryTimeInSeconds) {
        this.expiryTimeInSeconds = expiryTimeInSeconds;
    }

    /**
     * Get all nodes of the cluster
     * 
     * @return list of all configured {@link QuasardbNode}
     * @since 1.1.5
     */
    public Collection<QuasardbNode> getNodes() {
        return Collections.unmodifiableList(this.nodes);
    }
    
    /**
     * Add a new {@link QuasardbNode} to the cluster configuration
     * 
     * @param node node to add
     * @since 1.1.5
     */
    public void addNode(QuasardbNode node) {
        this.nodes.add(node);
    }
    
    /**
     * Remove a {@link QuasardbNode} from the cluster configuration.
     * 
     * @param node node to remove from the cluster ring. 
     * @return true if the provided node has been removed
     * @since 1.1.5
     */
    public boolean removeNode(QuasardbNode node) {
        return nodes.remove(node);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#hashCode()
     * @since 1.3.0
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (expiryTimeInSeconds ^ (expiryTimeInSeconds >>> 32));
        result = prime * result + ((nodes == null) ? 0 : nodes.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 1.3.0
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QuasardbConfig other = (QuasardbConfig) obj;
        if (expiryTimeInSeconds != other.expiryTimeInSeconds)
            return false;
        if (nodes == null) {
            if (other.nodes != null)
                return false;
        } else if (!nodes.equals(other.nodes))
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     * @since 1.3.0
     */
    @Override
    public String toString() {
        return "QuasardbConfig [expiryTimeInSeconds=" + expiryTimeInSeconds + ", nodes=<" + nodes.toString() + ">]";
    }
    
}
