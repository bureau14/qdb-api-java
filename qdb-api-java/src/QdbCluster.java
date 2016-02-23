package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;

/**
 * A connection to a quasardb cluser.
 */
public final class QdbCluster {
    private static CharsetDecoder utf8 = Charset.forName("UTF-8").newDecoder();
    private transient SWIGTYPE_p_qdb_session session;

    static {
        QdbNativeApi.load();
    }

    /**
     * Connects to a quasardb cluster through the specified URI.
     * The URI contains the addresses of the bootstraping nodes, other nodes are discovered during the first connection.
     * Having more than one node in the URI allows to connect to the cluster even if the first node is down.
     *
     * @param uri a string in the form of <code>qdb://&lt;address1&gt;:&lt;port1&gt;[,&lt;address2&gt;:&lt;port2&gt;...]</code>
     * @throws QdbConnectionRefusedException If the connection to the cluster is refused.
     * @throws QdbInvalidArgumentException If the syntax of the URI is incorrect.
     */
    public QdbCluster(String uri) {
        session = qdb.open();
        qdb_error_t err = qdb.connect(session, uri);
        QdbExceptionThrower.throwIfError(err);
    }

    private void checkSession() {
        if (session == null) {
            throw new QdbMiscException(qdb_error_t.error_not_connected);
        }
    }

    /**
     * Retrieve the version of the current quasardb instance.
     *
     * @return version of the current quasardb instance.
     */
    public String getVersion() {
        this.checkSession();
        return qdb.version();
    }

    /**
     * Retrieve the build version of the current quasardb instance.
     *
     * @return build version of the current quasardb instance.
     */
    public String getBuild() {
        this.checkSession();
        return qdb.build();
    }

    /**
     * Returns the low-level, underlying, session object.
     * This is reserved for advanced user who want to use the JNI API.
     *
     * @return the low-level, underlying, session object
     *
     */
    public SWIGTYPE_p_qdb_session getSession() {
        return session;
    }

    /**
     * Close connetion to the database.
     */
    public void disconnect() {
        qdb_error_t err = qdb.close(session);
        QdbExceptionThrower.throwIfError(err);
        this.session = null;
    }

    /**
     * Check if client is connected to quasardb instance.
     *
     * @return true if client is connected to quasardb instance
     */
    public boolean isConnected() {
        try {
            return !this.getVersion().isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Retrieve the status of the current quasardb instance in JSON format.
     * <br>
     * JSON response has the following format :
     * <pre>
     * {
     *       "engine_build_date":"87f8d02 2014-01-15 16:12:30 +0100",
     *       "engine_version":"master",
     *       "entries":
     *       {
     *          "persisted":{"count":0,"size":0},
     *           "resident":{"count":0,"size":0}
     *       },
     *       "hardware_concurrency":6,
     *       "memory":
     *       {
     *           "physmem":{"total":25767272448,"used":5493329920},
     *           "vm":{"total":8796092891136,
     *           "used":417980416}
     *       },
     *       "network":{
     *           "listening_address":"127.0.0.1",
     *           "listening_port":2836,
     *           "partitions":
     *           {
     *               "available_sessions":[1999,1999,2000,2000,2000],
     *               "count":5,
     *               "max_sessions":2000
     *           }
     *       },
     *       "node_id":"5309f39a3f176b9-179cd55bd9dc83e5-c09beea926e4bb75-a460c8c4e5487da9",
     *       "operating_system":"Microsoft Windows 7 Ultimate Edition Service Pack 1 (build 7601), 64-bit",
     *       "operations":
     *       {
     *           "compare_and_swap":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *              "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *           },
     *           "find":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *               "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *           },
     *           "find_remove":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *               "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *           },
     *           "find_update":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *               "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *           },
     *           "put":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *               "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *           },
     *           "remove":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *               "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *           },
     *           "purge_all":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *               "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *          },
     *           "remove_if":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *               "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *           },
     *           "update":{
     *               "count":0,
     *               "evictions":0,
     *               "failures":0,
     *               "in_bytes":0,
     *               "out_bytes":0,
     *               "pageins":0,
     *               "successes":0
     *          }
     *       },
     *       "overall":{
     *           "count":0,
     *           "evictions":0,
     *           "failures":0,
     *           "in_bytes":0,
     *           "out_bytes":0,
     *           "pageins":0,
     *           "successes":0
     *       },
     *       "startup":"2014-01-20T15:01:11",
     *       "timestamp":"2014-01-20T15:09:40"
     * }
     * </pre>
     *
     * @param uri The address of the node
     * @return status of the current quasardb instance in JSON (see below)
     * @throws QdbInvalidArgumentException If the syntax of the URI is incorrect.
     */
    public String getNodeStatus(String uri) {
        error_carrier error = new error_carrier();
        ByteBuffer buffer = qdb.node_status(session, uri, error);
        try {
            return utf8.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new QdbUnexpectedReplyException(e.getMessage());
        } finally {
            qdb.free_buffer(session, buffer);
        }
    }

    /**
     * Retrieve the configuration of the specific quasardb instance in JSON.
     * <br>
     * JSON response has the following format :
     * <pre>
     * {
     *       "local":{
     *           "depot":{
     *               "root":"db",
     *               "sync":false
     *           },
     *           "user":{
     *               "license_file":"qdb.lic"
     *           },
     *           "limiter":{
     *               "max_bytes":12883636224,
     *               "max_resident_entries":100000
     *           },
     *           "logger":{
     *               "flush_interval":3,
     *               "log_level":2,
     *               "log_to_console":true,
     *               "log_to_syslog":false
     *           },
     *           "network":{
     *               "client_timeout":60,
     *               "idle_timeout":300,
     *               "listen_on":"127.0.0.1:2836",
     *               "partitions_count":5,
     *               "server_sessions":2000
     *           },
     *           "chord":{
     *               "bootstrapping_peers":[],
     *               "no_stabilization":false,
     *               "node_id":"5309f39a3f176b9-179cd55bd9dc83e5-c09beea926e4bb75-a460c8c4e5487da9"
     *           }
     *       },
     *       "global":{
     *           "cluster":{
     *               "replication_factor":1,
     *               "transient":false
     *           }
     *       }
     * }
     * </pre>
     *
     * @param uri The address of the node
     * @return configuration of the current quasardb instance.
     * @throws QdbInvalidArgumentException If the syntax of the URI is incorrect.
     */
    public String getNodeConfig(String uri) {
        error_carrier error = new error_carrier();
        ByteBuffer buffer = qdb.node_config(session, uri, error);
        try {
            return utf8.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new QdbUnexpectedReplyException(e.getMessage());
        } finally {
            qdb.free_buffer(session, buffer);
            buffer = null;
        }
    }

    /**
     * Retrieve the topology of the current quasardb instance.
     * <br>
     * JSON response has the following format :
     * <pre>
     * {
     *       "center":{
     *           "endpoint":"127.0.0.1:2836",
     *           "reference":"5309f39a3f176b9-179cd55bd9dc83e5-c09beea926e4bb75-a460c8c4e5487da9"
     *       },
     *       "predecessor":{
     *           "endpoint":"127.0.0.1:2836",
     *           "reference":"5309f39a3f176b9-179cd55bd9dc83e5-c09beea926e4bb75-a460c8c4e5487da9"
     *       },
     *       "successor":{
     *           "endpoint":"127.0.0.1:2836",
     *           "reference":"5309f39a3f176b9-179cd55bd9dc83e5-c09beea926e4bb75-a460c8c4e5487da9"
     *       }
     * }
     *
     * </pre>
     *
     * @param uri the host of the quasardb node you want to retrieve topology
     * @return topology of the current quasardb instance.
     * @throws QdbInvalidArgumentException If the syntax of the URI is incorrect.
     */
    public String getNodeTopology(String uri) {
        error_carrier error = new error_carrier();
        ByteBuffer buffer = qdb.node_topology(session, uri, error);
        try {
            return utf8.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new QdbUnexpectedReplyException(e.getMessage());
        } finally {
            qdb.free_buffer(session, buffer);
            buffer = null;
        }
    }

    /**
     * Stop a quasardb node of the cluster.
     *
     * @param uri address of node to stop.
     * @param reason administration message which is a notification why the node was stopped.
     * @return true when node was stopped.
     * @throws QdbInvalidArgumentException If the syntax of the URI is incorrect.
     */
    public boolean stopNode(String uri, String reason) {
        qdb_error_t err = qdb.stop_node(session, uri, reason);
        if (err != qdb_error_t.error_ok) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Remove all data from the cluster.
     *
     * @throws QdbOperationDisabledException If the purgeAll operation has been disabled on the server.
     */
    public void purgeAll() {
        qdb_error_t err = qdb.purge_all(session);
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Trim data from the cluster, that is, remove dead references and old versions.
     */
    public void trimAll() {
        qdb_error_t err = qdb.trim_all(session);
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
        RemoteNode location = qdb.get_location(session, alias, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return new QdbNode(location.getAddress(), location.getPort());
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
     * Get a handle to a integer in the database.
     *
     * @param alias The entry unique key/identifier in the database.
     * @return A handle to perform operations on the integer.
     */
    public QdbInteger getInteger(String alias) {
        return new QdbInteger(session, alias);
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
        return new QdbBatch(session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        try {
            return "QdbCluster - Version : " + this.getVersion() + " - Build : " + this.getBuild();
        } catch (Exception e) {
            return "QdbCluster - Error => " + e.getMessage();
        }
    }
}
