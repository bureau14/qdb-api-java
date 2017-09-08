package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.util.regex.*;
import java.util.List;
import net.quasardb.qdb.jni.*;

/**
 * A connection to a quasardb cluster.
 */
public final class QdbCluster implements AutoCloseable {
    private final QdbSession session;

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
        int err = qdb.connect(session.handle(), uri);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Closes the connection to the cluster.
     */
    public void close() {
        session.close();
    }

    /**
     * Gets a handle to a blob in the database.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the blob.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public QdbBlob blob(String alias) {
        session.throwIfClosed();
        return new QdbBlob(session, alias);
    }

    /**
       * Gets a handle to a deque (double-ended queue) in the database.
       *
       * @param alias The entry unique key/identifier in the database.
       * @return A handle to perform operations on the deque.
       * @throws QdbClusterClosedException If QdbCluster.close() has been called.
       */
    public QdbDeque deque(String alias) {
        session.throwIfClosed();
        return new QdbDeque(session, alias);
    }

    /**
       * Gets a handle to an entry in the database, the entry must exist.
       *
       * @param alias The entry unique key/identifier in the database.
       * @return A subclass of QdbEntry depending on the type of entry currently in the database.
       * @throws QdbAliasNotFoundException If the entry does not exist.
       * @throws QdbClusterClosedException If QdbCluster.close() has been called.
       */
    public QdbEntry entry(String alias) {
        session.throwIfClosed();
        return new QdbEntryFactory(session).createEntry(alias);
    }

    /**
       * Gets a handle to a hash-set in the database.
       *
       * @param alias The entry unique key/identifier in the database.
       * @return A handle to perform operations on the hash-set.
       * @throws QdbClusterClosedException If QdbCluster.close() has been called.
       */
    public QdbHashSet hashSet(String alias) {
        session.throwIfClosed();
        return new QdbHashSet(session, alias);
    }

    /**
     * Gets a handle to an integer in the database.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the integer.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public QdbInteger integer(String alias) {
        session.throwIfClosed();
        return new QdbInteger(session, alias);
    }

    /**
       * Gets a handle to a node (i.e. a server) in the cluster.
       *
       * @param uri The URI of the node, in the form qdb://10.0.0.1:2836
       * @return The node
       * @throws QdbClusterClosedException If QdbCluster.close() has been called.
       */
    public QdbNode node(String uri) {
        Pattern pattern = Pattern.compile("^qdb://(.*):(\\d+)/?$");
        Matcher matcher = pattern.matcher(uri);

        if (!matcher.find())
            throw new QdbInvalidArgumentException("URI format is incorrect.");

        String hostName = matcher.group(1);
        int port = Integer.parseInt(matcher.group(2));

        session.throwIfClosed();
        return new QdbNode(session, hostName, port);
    }

    /**
      * Gets a handle to a stream in the database.
      *
      * @param alias The entry unique key/identifier in the database.
      * @return A handle to perform operations on the stream.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      */
    public QdbStream stream(String alias) {
        session.throwIfClosed();
        return new QdbStream(session, alias);
    }

    /**
     * Get a handle to a tag to in the database
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the tag.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public QdbTag tag(String alias) {
        session.throwIfClosed();
        return new QdbTag(session, alias);
    }

    /**
      * Gets a handle to a timeseries in the database.
      *
      * @param alias The timeseries unique key/identifier in the database.
      * @return A handle to perform operations on the timeseries.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      */
    public QdbTimeSeries timeSeries(String alias) {
        session.throwIfClosed();
        return new QdbTimeSeries(session, alias);
    }

    /**
      * Gets creates a new timeseries in the database and returns handle.
      *
      * @param alias The timeseries unique key/identifier in the database.
      * @return A handle to perform operations on the timeseries.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      */
    public QdbTimeSeries createTimeSeries(String alias, List<QdbTimeSeries.ColumnDefinition> columns) {
        session.throwIfClosed();
        QdbTimeSeries series = new QdbTimeSeries(session, alias);
        series.create(columns);
        return series;
    }

    /**
       * Retrieve the location for a provided alias.
       *
       * @param alias The entry unique key/identifier in the database.
       * @return the location, i.e. node's address and port, on which the entry with the provided alias is stored.
       * @throws QdbClusterClosedException If QdbCluster.close() has been called.
       */
    public QdbNode findNodeFor(String alias) {
        QdbEntry e = new QdbEntry(session, alias);
        return e.node();
    }

    /**
     * Remove all data from the cluster.
     *
     * @param timeoutMillis The timeout of the operation, in milliseconds
     *
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbOperationDisabledException If the operation has been disabled on the server.
     */
    public void purgeAll(int timeoutMillis) {
        session.throwIfClosed();
        int err = qdb.purge_all(session.handle(), timeoutMillis);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Trim data from the cluster, that is, remove dead references and old versions.
     *
     * @param timeoutMillis The timeout of the operation, in milliseconds
     *
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public void trimAll(int timeoutMillis) {
        session.throwIfClosed();
        int err = qdb.trim_all(session.handle(), timeoutMillis);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Create an empty batch.
     *
     * @return An empty batch.
     */
    public QdbBatch createBatch() {
        session.throwIfClosed();
        return new QdbBatch(session);
    }

    /**
     * Set network timeout for this client.
     *
     * @param timeoutMillis The timeout of the operation, in milliseconds
     *
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public void setTimeout(int timeoutMillis) {
        session.throwIfClosed();
        int err = qdb.option_set_timeout(session.handle(), timeoutMillis);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Retrieve the build version of the quasardb API
     *
     * @return build version of the quasardb API
     */
    public static String build() {
        return qdb.build();
    }

    /**
     * Retrieve the version of the quasardb API
     *
     * @return The version of the quasardb API.
     */
    public static String version() {
        return qdb.version();
    }
}
