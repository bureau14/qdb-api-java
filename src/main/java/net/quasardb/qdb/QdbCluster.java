package net.quasardb.qdb;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.quasardb.qdb.jni.RemoteNode;
import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.StringVec;
import net.quasardb.qdb.jni.error_carrier;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.tools.LibraryHelper;

/**
 * Represents a connection to a quasardb cluster.
 * 
 * @author &copy; <a href="https://www.quasardb.net">quasardb</a> - 2015
 * @version 2.0.0
 * @since 2.0.0
 */
public final class QdbCluster {
    private static final String QDB_URI_REGEXP = "^qdb://([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5]):\\d+";
    private static final String EMPTY_ALIAS = "Alias shouldn't be null or empty.";
    private static final Charset charset = Charset.forName("UTF-8");
    private static final CharsetDecoder decoder = charset.newDecoder();
    static {
        // Try to load qdb library
        try {
            // Load library from LD_LIBRARY_PATH
            System.loadLibrary("qdb_java_api"); //NOPMD
        } catch (UnsatisfiedLinkError e) {
            // Load library from Jar if LD_LIBRARY_PATH not set
            LibraryHelper.loadLibrairiesFromJar();
        }
    }
    
    private final Pattern qdbUriPattern = Pattern.compile(QDB_URI_REGEXP);
    private transient SWIGTYPE_p_qdb_session session;
    
    /**
     * Connects to a quasardb cluster through the specified URI. 
     * The URI contains the addresses of the bootstraping nodes, other nodes are discovered during the first connection. 
     * Having more than one node in the URI allows to connect to the cluster even if the first node is down.
     * 
     * @param uri a string in the form of <code>qdb://&lt;address1&gt;:&lt;port1&gt;[,&lt;address2&gt;:&lt;port2&gt;...]</code>
     * @throws URISyntaxException 
     * @throws QdbException
     */
    public QdbCluster(String uri) throws URISyntaxException, QdbException {
        if (this.validateURI(uri)) {
            // Try to open a qdb session
            session = qdb.open();
            
            // Try to connect to quasardb cluster
            final qdb_error_t qdbError = qdb.connect(session, uri);            
            if (qdbError != qdb_error_t.error_ok) {
                throw new QdbException(qdbError);
            }
        } else {
            throw new URISyntaxException(uri, "qdb uri should be in the form of qdb://<address1>:<port1>[,<address2>:<port2>...]");
        }
    }
    
    /**
     * 
     * @throws URISyntaxException
     * @throws QdbException
     */
    public QdbCluster() throws URISyntaxException, QdbException {
        this("qdb://127.0.0.1:2836");
    }
    
    /**
     * Check the syntax of a provided qdb URI.<br>
     * This doesn't validate if addresses/port are alive.
     * 
     * @param uri a qdb uri to check
     * @return true if provided uri is a string in the form of <code>qdb://&lt;address1&gt;:&lt;port1&gt;[,&lt;address2&gt;:&lt;port2&gt;...]</code>. False in all other cases.
     */
    private final boolean validateURI(String uri)  {
        if ((uri == null) || uri.isEmpty()) {
            return false;
        } else {
            final Matcher qdbUriMatcher = qdbUriPattern.matcher(uri);
            return qdbUriMatcher.matches();
        }
    }
    
    /**
     * Check if the current qdb session is valid
     *
     * @throws QuasardbException if the connection to the qdb instance cannot be closed
     * @since 0.5.2
     */
    private final void checkSession() throws QdbException {
        if (session == null) {
            throw new QdbException(qdb_error_t.error_not_connected);
        }
    }

    /**
     * Retrieve the version of the current quasardb instance.
     *
     * @return version of the current quasardb instance.
     * @throws QdbException 
     * @since 2.0.0
     */
    public String getVersion() throws QdbException {
        this.checkSession();
        return qdb.version();
    }
    
    /**
     * Retrieve the build version of the current quasardb instance.
     *
     * @return build version of the current quasardb instance.
     * @throws QdbException 
     * @since 2.0.0
     */
    public String getBuild() throws QdbException {
        this.checkSession();
        return qdb.build();
    }

    /**
     *
     * @return the low-level, underlying, session object
     *
     * @since 2.0.0
     */
    public SWIGTYPE_p_qdb_session getSession() {
      return session;
     }
    
    /**
     * 
     * @throws QdbException
     */
    public void disconnect() throws QdbException {
        // Try to close qdb session
        final qdb_error_t qdbError = qdb.close(session);
        
        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
        
        this.session = null;
    }
    
    /**
     * Check if client is connected to quasardb instance.
     *
     * @return true if client is connected to quasardb instance
     */
    public final boolean isConnected() {
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
     * @param uri 
     * @return status of the current quasardb instance in JSON (see below)
     * @throws QdbException if the connection with the current instance fail.
     * @throws URISyntaxException
     * @since 0.7.4
     */
    public String getNodeStatus(final String uri) throws URISyntaxException, QdbException {
        if (validateURI(uri)) {
            String result = "";
            final error_carrier error = new error_carrier();
            ByteBuffer buffer = qdb.node_status(session, uri, error);
            try {
                if (buffer != null) {
                    result = decoder.decode(buffer).toString();
                }            
            } catch (CharacterCodingException e) {
                throw new QdbException(e);
            } finally {
                qdb.free_buffer(session, buffer);
                buffer = null;
            }
    
            return result;
        } else {
            throw new URISyntaxException(uri, "qdb uri should be in the form of qdb://<address1>:<port1>[,<address2>:<port2>...]");
        }
    }
    
    /**
     * Retrieve the configuration of the specific quasardb instance in JSON.
     * <br>
     * JSON response has the following format :
     * <pre>
     * {
     *       "global":{
     *           "depot":{
     *               "replication_factor":1,
     *               "root":"db",
     *               "sync":false,
     *               "transient":false
     *           },
     *           "limiter":{
     *               "max_bytes":12883636224,
     *               "max_in_entries_count":100000
     *           }
     *       },
     *       "local":{
     *           "chord":{
     *               "bootstrapping_peers":[],
     *               "no_stabilization":false,
     *               "node_id":"5309f39a3f176b9-179cd55bd9dc83e5-c09beea926e4bb75-a460c8c4e5487da9"
     *           },
     *           "logger":{
     *               "dump_file":"qdb_error_dump.txt",
     *               "flush_interval":3,
     *               "log_files":[],
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
     *           "user":{
     *               "license_file":"qdb.lic"
     *           }
     *       }
     * }
     * </pre>
     *
     * @param uri 
     * @return configuration of the current quasardb instance.
     * @throws QdbException if the connection with the current instance fail.
     * @throws URISyntaxException
     * @since 0.7.4
     */
    public String getNodeConfig(final String uri) throws URISyntaxException, QdbException {
        if (validateURI(uri)) {
            String result = "";
            final error_carrier error = new error_carrier();
            ByteBuffer buffer = qdb.node_config(session, uri, error);
            try {
                if (buffer != null) {
                    result = decoder.decode(buffer).toString();
                }            
            } catch (CharacterCodingException e) {
                throw new QdbException(e);
            } finally {
                qdb.free_buffer(session, buffer);
                buffer = null;
            }
    
            return result;
        } else {
            throw new URISyntaxException(uri, "qdb uri should be in the form of qdb://<address1>:<port1>[,<address2>:<port2>...]");
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
     * @throws QdbException if the connection with the current instance fail.
     * @throws URISyntaxException
     * @since 0.7.4
     */
    public String getNodeTopology(final String uri) throws URISyntaxException, QdbException {
        if (validateURI(uri)) {
            String result = "";
            final error_carrier error = new error_carrier();
            ByteBuffer buffer = qdb.node_topology(session, uri, error);
            try {
                if (buffer != null) {
                    result = decoder.decode(buffer).toString();
                }            
            } catch (CharacterCodingException e) {
                throw new QdbException(e);
            } finally {
                qdb.free_buffer(session, buffer);
                buffer = null;
            }
    
            return result;
        } else {
            throw new URISyntaxException(uri, "qdb uri should be in the form of qdb://<address1>:<port1>[,<address2>:<port2>...]");
        }
    }
    
    /**
     * Stop a quasardb node of the cluster.
     * 
     * @param uri address of node to stop.
     * @param reason administration message which is a notification why the node was stopped.
     * @return true when node was stopped.
     * @throws URISyntaxException when provided uri wasn't in the form of <code>qdb://&lt;address1&gt;:&lt;port1&gt;[,&lt;address2&gt;:&lt;port2&gt;...]</code>
     */
    public boolean stopNode(String uri, String reason) throws URISyntaxException {
        if (validateURI(uri)) {
            final qdb_error_t qdbError = qdb.stop_node(session, uri, reason);
            if (qdbError != qdb_error_t.error_ok) {
                return false;
            } else {
                return true;
            }
        } else {
            throw new URISyntaxException(uri, "qdb uri should be in the form of qdb://<address1>:<port1>[,<address2>:<port2>...]");
        }
    }
    
    /**
     * Remove all data from the cluster.
     * 
     * @throws QdbException 
     * @since 2.0.0
     */
    public void purgeAll() throws QdbException {
    	final qdb_error_t qdbError = qdb.purge_all(session);
    	if (qdbError != qdb_error_t.error_ok) {
    		throw new QdbException(qdbError);
    	}
	}
    
    /**
     * 
     * @param alias
     * @throws QdbException
     * @since 2.0.0
     */
    public void removeEntry(String alias) throws QdbException {
        final qdb_error_t qdbError = qdb.remove(session, alias);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
    }
        
    /**
     * Retrieve the location for a provided alias.
     *
     * @param alias the object's unique key/alias.
     * @return the location, i.e. node's address and port, on which the entry with the provided alias is stored.
     * @throws QuasardbException if the connection with current instance fail or provided alias doesn't exist or provided alias is reserved.
     * @since 2.0.0
     */
    public QdbNode getKeyLocation(final String alias) throws QdbException {
        final error_carrier error = new error_carrier();
        RemoteNode location = qdb.get_location(session, alias, error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return new QdbNode(location.getAddress(), location.getPort());
    }
    
    /**
     * @param alias
     * @return QdbInteger
     * @throws QdbException 
     */
    public QdbInteger getInteger(String alias) throws QdbException {
        return new QdbInteger(session, alias);
    }
    
    /**
     * 
     * @param alias
     * @return QdbBlob
     * @throws QdbException
     */
    public QdbBlob getBlob(String alias) throws QdbException {
        return new QdbBlob(session, alias);
    }
    
    /**
     * 
     * @param alias
     * @return QdbHashSet
     * @throws QdbException
     */
    public QdbHashSet getSet(String alias) throws QdbException {
    	return new QdbHashSet(session, alias);
    }
    
    /**
     * 
     * @param alias
     * @return QdbQueue
     * @throws QdbException
     */
    public QdbQueue getQueue(String alias) throws QdbException {
    	return new QdbQueue(session, alias);
    }
    
    /**
     * 
     * @param alias
     * @return QdbTag
     * @throws QdbException
     */
    public QdbTag getTag(String alias) throws QdbException {
        return new QdbTag(session, alias);
    }

    /**
     * 
     * @return QdbBatch
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
        } catch (QdbException e) {
            return "QdbCluster - Error => " + e.getMessage();
        }
    }

}
