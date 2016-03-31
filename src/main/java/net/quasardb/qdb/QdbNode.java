package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import net.quasardb.qdb.jni.*;

/**
 * A node in the quasardb cluster.
 */
public final class QdbNode {
    private static CharsetDecoder utf8 = Charset.forName("UTF-8").newDecoder();
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
     */
    public String config() {
        error_carrier error = new error_carrier();
        ByteBuffer buffer = qdb.node_config(session.handle(), uri, error);
        QdbExceptionFactory.throwIfError(error);

        // workaround: remove null terminator:
        buffer.limit(buffer.limit() - 1);

        try {
            return utf8.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new QdbUnexpectedReplyException(e.getMessage());
        } finally {
            qdb.free_buffer(session.handle(), buffer);
            buffer = null;
        }
    }

    /**
     * Retrieve the topology of the node.
     *
     * @return A JSON string containing the topology of the node.
     */
    public String topology() {
        error_carrier error = new error_carrier();
        ByteBuffer buffer = qdb.node_topology(session.handle(), uri, error);
        QdbExceptionFactory.throwIfError(error);

        // workaround: remove null terminator:
        buffer.limit(buffer.limit() - 1);

        try {
            return utf8.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new QdbUnexpectedReplyException(e.getMessage());
        } finally {
            qdb.free_buffer(session.handle(), buffer);
            buffer = null;
        }
    }

    /**
     * Retrieve the status of the node.
     *
     * @return A JSON string containing the status of the node.
     */
    public String status() {
        error_carrier error = new error_carrier();
        ByteBuffer buffer = qdb.node_status(session.handle(), uri, error);
        QdbExceptionFactory.throwIfError(error);

        // workaround: remove null terminator:
        buffer.limit(buffer.limit() - 1);

        try {
            return utf8.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new QdbUnexpectedReplyException(e.getMessage());
        } finally {
            qdb.free_buffer(session.handle(), buffer);
        }
    }

    /**
     * Shutdown the node.
     *
     * @param reason A message that will be logged as the reason for the shutdown.
     */
    public void stop(String reason) {
        qdb_error_t err = qdb.node_stop(session.handle(), uri, reason);
        QdbExceptionFactory.throwIfError(err);
    }
}
