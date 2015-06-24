package com.b14.qdb;

/**
 * A quasardb <a href="https://doc.quasardb.net/1.1.4/glossary.html#term-node">node</a> is a <i>{hostname, port}</i> tuple.
 *  
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version 2.0.0
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
    
}
