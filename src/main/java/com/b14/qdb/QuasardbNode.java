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

/**
 * A quasardb <a href="https://doc.quasardb.net/1.1.4/glossary.html#term-node">node</a> is a <i>{hostname, port}</i> tuple.
 *  
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.5
 */
public class QuasardbNode {
    // Hostname of quasardb node
    private String hostName;
    
    // Port of quasardb node
    private int port;
    
    /**
     * Default constructor. Build a node with default values. Default values are :
     * <ul>
     *  <li>hostname => 127.0.0.1</li>
     *  <li>port => 2836</li>
     * </ul>
     * 
     * @since 1.1.5
     */
    public QuasardbNode() {
        this.hostName = "127.0.0.1";
        this.port = 2836;
    }
    
    /**
     * Build a QuasardbNode with a provided hostname and port.
     * 
     * @param hostName hostname of the quasardb node. Example : 127.0.0.1
     * @param port port of the quasardb node. Example : 2836
     * @since 1.1.5
     */
    public QuasardbNode(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
    }
    
    /**
     * Get the hostname of the current quasardb node.
     * 
     * @return the hostname of the quasardb node
     * @since 1.1.5
     */
    public String getHostName() {
        return this.hostName;
    }
    
    /**
     * Set hostname for a quasardb node.
     * Example : 127.0.0.1
     * 
     * <b>N.B :</b>hostname can be a logical name 
     * 
     * @param hostName the host to set
     * @since 1.1.5
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
    
    /**
     * Get the port of the current quasardb node.
     * 
     * @return the port of the quasardb node
     * @since 1.1.5
     */
    public int getPort() {
        return this.port;
    }
    
    /**
     * Set port for a quasardb node.
     * 
     * @param port the quasardb node port to set to the node
     * @since 1.1.5
     */
    public void setPort(int port) {
        this.port = port;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     * @since 1.3.0
     */
    @Override
    public String toString() {
        return this.hostName + ":" + this.port;
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
        result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
        result = prime * result + port;
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
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        QuasardbNode other = (QuasardbNode) obj;
        if (hostName == null) {
            if (other.hostName != null) {
                return false;
            }
        } else if (!hostName.equals(other.hostName)) {
            return false;
        }
        if (port != other.port) {
            return false;
        }
        return true;
    }
    
}
