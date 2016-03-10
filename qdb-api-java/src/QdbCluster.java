package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.util.regex.*;
import net.quasardb.qdb.jni.*;

/**
 * A connection to a quasardb cluster.
 */
public final class QdbCluster {
    private transient QdbSession session;

    /**
     * Connects to a quasardb cluster through the specified URI.
     * The URI contains the addresses of the bootstrapping nodes, other nodes are discovered during the first connection.
     * Having more than one node in the URI allows to connect to the cluster even if the first node is down.
     *
     * @param uri a string in the form of <code>qdb://&lt;address1&gt;:&lt;port1&gt;[,&lt;address2&gt;:&lt;port2&gt;...]</code>
     * @throws QdbConnectionRefusedException If the connection to the cluster is refused.
     * @throws QdbInvalidArgumentException If the syntax of the URI is incorrect.
     */
    public QdbCluster(String uri) {
        session = new QdbSession();
        qdb_error_t err = qdb.connect(session.handle(), uri);
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Retrieve the version of the current quasardb instance.
     *
     * @return version of the current quasardb instance.
     */
    public static String getVersion() {
        return qdb.version();
    }

    /**
     * Retrieve the build version of the current quasardb instance.
     *
     * @return build version of the current quasardb instance.
     */
    public static String getBuild() {
        return qdb.build();
    }

    /**
     * Remove all data from the cluster.
     *
     * @throws QdbOperationDisabledException If the operation has been disabled on the server.
     */
    public void purgeAll() {
        qdb_error_t err = qdb.purge_all(session.handle());
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Trim data from the cluster, that is, remove dead references and old versions.
     */
    public void trimAll() {
        qdb_error_t err = qdb.trim_all(session.handle());
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Retrieve the location for a provided alias.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return the location, i.e. node's address and port, on which the entry with the provided alias is stored.
     */
    public QdbNode getKeyLocation(String alias) {
        error_carrier error = new error_carrier();
        RemoteNode location = qdb.get_location(session.handle(), alias, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return new QdbNode(session, location.getAddress(), location.getPort());
    }

    /**
     * Get a handle to a blob in the database.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the blob.
     */
    public QdbBlob getBlob(String alias) {
        return new QdbBlob(session, alias);
    }

    /**
     * Get a handle to a deque (double-ended queue) in the database.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the deque.
     */
    public QdbDeque getDeque(String alias) {
        return new QdbDeque(session, alias);
    }

    /**
     * Get a handle to an integer in the database.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the integer.
     */
    public QdbInteger getInteger(String alias) {
        return new QdbInteger(session, alias);
    }

    /**
     * Get a handle to a node (i.e.<!-- --> a server) in the cluster.
     *
     * @param uri The URI of the node, in the form qdb://10.0.0.1:2836
     * @return The node
     */
    public QdbNode getNode(String uri) {
        Pattern pattern = Pattern.compile("^qdb://(.*):(\\d+)/?$");
        Matcher matcher = pattern.matcher(uri);

        if (!matcher.find())
            throw new QdbInvalidArgumentException("URI format is incorrect.");

        String hostName = matcher.group(1);
        int port = Integer.parseInt(matcher.group(2));

        return new QdbNode(session, hostName, port);
    }

    /**
     * Get a handle to a hash-set in the database.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the hash-set.
     */
    public QdbHashSet getSet(String alias) {
        return new QdbHashSet(session, alias);
    }

    /**
     * Get a handle to a stream in the database.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the stream.
     */
    public QdbStream getStream(String alias) {
        return new QdbStream(session, alias);
    }

    /**
     * Get a tag to a hash-set in the database
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the tag.
     */
    public QdbTag getTag(String alias) {
        return new QdbTag(session, alias);
    }

    /**
     * Create an empty batch.
     *
     * @return An empty batch.
     */
    public QdbBatch createBatch() {
        return new QdbBatch(session.handle());
    }
}
