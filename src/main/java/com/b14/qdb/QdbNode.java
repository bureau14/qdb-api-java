package com.b14.qdb;

/**
 * A quasardb <a href="https://doc.quasardb.net/1.1.4/glossary.html#term-node">node</a> is a <i>{hostname, port}</i> tuple.
 *  
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.5
 */
public class QdbNode {
    // Hostname of quasardb node
    private String hostName;
    
    // Port of quasardb node
    private int port;
    
    /**
     * Build a QuasardbNode with a provided hostname and port.
     * 
     * @param hostName hostname of the quasardb node. Example : 127.0.0.1
     * @param port port of the quasardb node. Example : 2836
     * @since 1.1.5
     */
    public QdbNode(String hostName, int port) {
        this.hostName = hostName;
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
        QdbNode other = (QdbNode) obj;
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
