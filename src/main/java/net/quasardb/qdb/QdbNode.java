package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

/**
 * A node in the quasardb cluster.
 */
public final class QdbNode {
    private transient QdbSession session;
    private final String hostName;
    private final int port;
    private final String uri;

    // Protected constructor. Call QdbCluster.node() to get an instance.
    protected QdbNode(QdbSession session, String hostName, int port) {
        this.session = session;
        this.hostName = hostName;
        this.port = port;
        uri = "qdb://" + hostName + ":" + port;
    }

    /**
     * Get the hostname of the node.
     *
     * @return the hostname of the quasardb node
     */
    public String hostName() {
        return this.hostName;
    }

    /**
     * Get the port of the node.
     *
     * @return the port of the quasardb node
     */
    public int port() {
        return this.port;
    }

    /**
     * Retrieve the configuration node.
     *
     * @return A JSON string containing the configuration of the node.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public String config() {
        session.throwIfClosed();
        Reference<String> config = new Reference<String>();
        int err = qdb.node_config(session.handle(), uri, config);
        QdbExceptionFactory.throwIfError(err);
        return config.value;
    }

    /**
     * Retrieve the status of the node.
     *
     * @return A JSON string containing the status of the node.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public String status() {
        session.throwIfClosed();
        Reference<String> status = new Reference<String>();
        int err = qdb.node_status(session.handle(), uri, status);
        QdbExceptionFactory.throwIfError(err);
        return status.value;
    }

    /**
     * Shutdown the node.
     *
     * @param reason A message that will be logged as the reason for the shutdown.
     */
    public void stop(String reason) {
        session.throwIfClosed();
        int err = qdb.node_stop(session.handle(), uri, reason);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Retrieve the topology of the node.
     *
     * @return A JSON string containing the topology of the node.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public String topology() {
        session.throwIfClosed();
        Reference<String> topology = new Reference<String>();
        int err = qdb.node_topology(session.handle(), uri, topology);
        QdbExceptionFactory.throwIfError(err);
        return topology.value;
    }
}
