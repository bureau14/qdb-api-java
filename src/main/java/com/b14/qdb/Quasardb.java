/**
 * Copyright (c) 2009-2013, Bureau 14 SARL
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
 *    * Neither the name of Bureau 14 nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
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

import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TimeZone;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import com.b14.qdb.batch.Operation;
import com.b14.qdb.batch.Result;
import com.b14.qdb.batch.Results;
import com.b14.qdb.batch.TypeOperation;
import com.b14.qdb.entities.NodeConfig;
import com.b14.qdb.entities.NodeStatus;
import com.b14.qdb.entities.NodeTopology;
import com.b14.qdb.entities.QuasardbEntry;
import com.b14.qdb.jni.BatchOpsVec;
import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.StringVec;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_const_iterator_t;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.jni.qdb_operation_t;
import com.b14.qdb.jni.qdb_operation_type_t;
import com.b14.qdb.jni.qdb_remote_node_t;
import com.b14.qdb.jni.run_batch_result;
import com.b14.qdb.tools.LibraryHelper;
import com.b14.qdb.tools.profiler.RamUsageEstimator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.shaded.org.objenesis.strategy.StdInstantiatorStrategy;
import com.owlike.genson.Genson;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;

/**
 * Quasardb main abstraction layer.<br>
 *
 * The following operations are allowed:
 *
 * <ul>
 *     <li><u>get:</u> get an entry.</li>
 *     <li><u>next:</u> get the next entry in the iteration</li>
 *     <li><u>hasNext:</u> is there a next entry in the iteration ?</li>
 *     <li><u>put:</u> create an entry.</li>
 *     <li><u>update:</u> update the value of an existing entry.</li>
 *     <li><u>getAndReplace:</u> atomically update the value of an existing entry and return the old value.</li>
 *     <li><u>compareAndSwap:</u> atomically compare a value with comparand and update if it matches. Always return the old value.</li>
 *     <li><u>remove:</u> delete an entry.</li>
 *     <li><u>removeAll:</u> delete all entries. Use with caution.</li>
 *     <li><u>removeIf:</u> delete the object associated whith a key if the object is equal to comparand.</li>
 *     <li><u>getRemove:</u> atomically get the entry associated with the supplied key and remove it.</li>
 *     <li><u>close:</u> close the connection.</li>
 *     <li><u>getVersion:</u> get API version.</li>
 *     <li><u>getBuild:</u> get API build number.</li>
 *     <li><u>getCurrentNodeConfig:</u> retrieve the configuration of the current quasardb instance.</li>
 *     <li><u>getCurrentNodeStatus:</u> retrieve the status of the current quasardb instance.</li>
 *     <li><u>getCurrentNodeTopology:</u> retrieve the topology of the current quasardb instance.</li>
 *     <li><u>runBatch:</u> can increase performance when it is necessary to run many small operations.</li>
 *     <li><u>stopCurrentNode:</u> stop the current quasardb instance.</li>
 *     <li><u>stopNode:</u> stop a provided quasardb instance.</li>
 *     TODO
 * </ul>
 *
 * <p>
 * <u>Usage example :</u>
 * <p>
 * <pre>
 *       // First : create a configuration map.
 *       Map<String,String> config = new HashMap<String,String>();
 *       config.put("name", "quasardb name");
 *       config.put("host", "quasardb host");
 *       config.put("port", "quasardb port");
 *
 *       // Second : create a related quasardb instance.
 *       Quasardb qdb = new Quasardb(config);
 *       // Or you can supply the configuration later :
 *       //   Quasardb qdb = new Quasardb();
 *       //   qdb.setConfig(config);
 *
 *       // Third : connect to quasardb cluster.
 *       qdb.connect();
 *
 *       // Fourth : use the quasardb instance :
 *       qdb.put("foo", new String("bar"));
 *       System.out.println("  => key 'foo' contains : " + qdb.get("foo"));
 *       
 *       // Fifth : disconnect from the quasardb cluster
 *       // Notice that this step is optional
 *       qdb.close();
 * </pre>
 * </p>
 *
 * @author &copy; <a href="https://www.bureau14.fr/">bureau14</a> - 2013
 * @version 1.1.0
 * @since 0.5.2
 */
@SuppressWarnings("restriction")
public final class Quasardb implements Iterable<QuasardbEntry<?>> {
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

    // Quasardb CONSTANTS
    private static final String NO_CONFIG_PROVIDED = "No config provided. Please call setConfig().";
    private static final String WRONG_CONFIG_PROVIDED = "Wrong config provided. Please check it.";
    private static final String SESSION_CLOSED = "Session was closed by peer.";
    private static final String NULL_VALUE = "Value is null. This is not allowed.";
    private static final String BAD_SIZE = "Object size is invalid.";
    private static final String WRONG_ALIAS = "Alias name is invalid.";
    private static final String WRONG_DATE = "Date is invalid.";
    private static final String BAD_SERIALIZATION = "Bad serialization.";
    private static final int BUFFER_SIZE = 4096;
    private static final int PUT = 1;
    private static final int UPDATE = 2;
    private static final int CAS = 3;
    private static final int GETANDUPDATE = 4;
    
    // ByteBuffer to String
    private static final Charset charset = Charset.forName("UTF-8");
    private static final CharsetDecoder decoder = charset.newDecoder();

    // Serializer initialisation
    private final Kryo serializer = new Kryo();
    
    // JSON Deserializer
    private final Genson genson = new Genson();

    // Keep qdb session reference
    private transient SWIGTYPE_p_qdb_session session;

    // Configuration of the qdb instance
    private transient Map<String,String> config = null;
    
    // Default expiry time in second => 0 means that entry is eternal
    private long defaultExpiryTime = 0;

    public Quasardb() {
    }

    /**
     * Create a quasardb instance with the provided configuration map.<br>
     * The configuration map must have the following parameters :
     * <ul>
     *     <li><i>name:</i> the unique name under which the quasardb instance will be referenced/registered.</li>
     *     <li><i>host:</i> the host of the quasardb cluster you want to connect to - can be a IP address or a hostname.</li>
     *     <li><i>port: </i> the port of the quasardb cluster you want to connect to.</li>
     *     <li><i>expiry: </i> the default expiry time in seconds for all new entries.</li>
     * </ul>
     * <br>
     *
     * <u>Example :</u>
     * <p>
     * <pre>
     *         // First : create a configuration map.
     *         Map<String,String> config = new HashMap<String,String>();
     *      config.put("name", "quasardb name");
     *      config.put("host", "quasardb host");
     *      config.put("port", "quasardb port");
     *      
     *      // Optionnaly set a default expiry time in seconds on all next entries
     *      config.put("expiry", "2");
     *
     *      // Second : create a related quasardb instance.
     *      Quasardb myQuasardbInstance = new Quasardb(config);
     *     </pre>
     * </p>
     *
     * @param config the config map in order to initialize connexion with the quasardb instance
     * @throws QuasardbException if initialization step fail
     */
    public Quasardb(final Map<String,String> config) {
        this.config = config;
    }

    /**
     * Initialize connection to the quasardb instance and setup serialization framework.
     *
     * @since 0.5.2
     *  
     * @throws QuasardbException if connection to the quasardb instance fail
     */
    public void connect() throws QuasardbException {
        // Check params
        if (config == null) {
            throw new QuasardbException(NO_CONFIG_PROVIDED);
        }

        if (config.get("host") == null) {
            throw new QuasardbException(WRONG_CONFIG_PROVIDED);
        }

        if ((config.get("port") == null)) {
            throw new QuasardbException(WRONG_CONFIG_PROVIDED);
        }

        try {
            Integer.parseInt(config.get("port"));
        } catch (NumberFormatException e) {
            throw new QuasardbException(WRONG_CONFIG_PROVIDED, e);
        }
        
        try {
            this.setDefaultExpiryTimeInSeconds(Long.parseLong(config.get("expiry")));
        } catch (NumberFormatException e) {
            this.setDefaultExpiryTimeInSeconds(0);
        }
        
        // Initialize serializer
        serializer.setRegistrationRequired(false);
        serializer.setReferences(false);
        serializer.setInstantiatorStrategy(new StdInstantiatorStrategy());
        serializer.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
        serializer.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
        serializer.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
        serializer.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
        serializer.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
        serializer.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
        serializer.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
        serializer.register(GregorianCalendar.class, new GregorianCalendarSerializer());
        serializer.register(InvocationHandler.class, new JdkProxySerializer());
        UnmodifiableCollectionsSerializer.registerSerializers(serializer);
        SynchronizedCollectionsSerializer.registerSerializers(serializer);

        // Try to open a qdb session
        session = qdb.open();

        // Try to connect to the qdb node
        final qdb_error_t qdbError = qdb.connect(session, config.get("host"), Integer.parseInt(config.get("port")));

        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }
    }

    /**
     * Retrieve the version of the current quasardb instance.
     *
     * @since 0.7.4
     *
     * @return version of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public String getVersion() throws QuasardbException {
        this.checkSession();
        return qdb.version();
    }

    /**
     * Retrieve the build version of the current quasardb instance.
     *
     * @since 0.7.4
     *
     * @return build version of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public String getBuild() throws QuasardbException {
        this.checkSession();
        return qdb.build();
    }

    /**
     * Retrieve the status of the current quasardb instance.
     *
     * @since 0.7.4
     * 
     * @return status of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public NodeStatus getCurrentNodeStatus() throws QuasardbException {
        try {
            return genson.deserialize(this.getNodeInfo(config.get("host"), Integer.parseInt(config.get("port")), "status"), NodeStatus.class);
        } catch (Exception e) {
            throw new QuasardbException(e);
        }
    }
    
    /**
     * Retrieve the status of the current quasardb instance.
     *
     * @since 0.7.4
     * 
     * @param node the host of the quasardb node you want to retrieve status
     * @param port the port of the quasardb node you want to retrieve status
     * @return status of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public NodeStatus getNodeStatus(final String node, final int port) throws QuasardbException {
        try {
            return genson.deserialize(this.getNodeInfo(node, port, "status"), NodeStatus.class);
        } catch (Exception e) {
            throw new QuasardbException(e);
        }
    }
    
    /**
     * Retrieve the configuration of the current quasardb instance.
     *
     * @since 0.7.4
     * 
     * @return configuration of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public NodeConfig getCurrentNodeConfig() throws QuasardbException {
        try {
            return genson.deserialize(this.getNodeInfo(config.get("host"), Integer.parseInt(config.get("port")), "config"), NodeConfig.class);
        } catch (Exception e) {
            throw new QuasardbException(e);
        }
    }
    
    /**
     * Retrieve the configuration of the specific quasardb instance.
     *
     * @since 0.7.4
     * 
     * @param node the host of the quasardb node you want to retrieve configuration
     * @param port the port of the quasardb node you want to retrieve configuration
     * @return configuration of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public NodeConfig getNodeConfig(final String node, final int port) throws QuasardbException {
        try {
            return genson.deserialize(this.getNodeInfo(node, port, "config"), NodeConfig.class);
        } catch (Exception e) {
            throw new QuasardbException(e);
        }
    }
    
    /**
     * Retrieve the topology of the current quasardb instance.
     *
     * @since 0.7.4
     * 
     * @return topology of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public NodeTopology getCurrentNodeTopology() throws QuasardbException {
        try {
            return genson.deserialize(this.getNodeInfo(config.get("host"), Integer.parseInt(config.get("port")), "topology"), NodeTopology.class);
        } catch (Exception e) {
            throw new QuasardbException(e);
        }
    }
    
    /**
     * Retrieve the expiry time in seconds for a provided alias
     * 
     * @since 1.1.0
     * 
     * @param alias the object's unique key/alias.
     * @return the expiry time in second related to the provided alias. 0 means eternal duration.
     * @throws QuasardbException if the connection with current instance fail or if provided alias doesn't exist.
     */
    public long getExpiryTimeInSeconds(final String alias) throws QuasardbException {
        // Checks params
        this.checkSession();
        this.checkAlias(alias);

        // Init
        long result = 0;
        error_carrier error = new error_carrier();
        
        // Get value associated with alias
        result = qdb.get_expiry(session, alias, error);
        
        // Handle errors
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QuasardbException(error.getError());
        }

        return result;
    }
    
    /**
     * Retrieve the expiry time in date for a provided alias
     * 
     * @since 1.1.0
     * 
     * @param alias the object's unique key/alias.
     * @return the expiry date related to the provided alias.
     * @throws QuasardbException if the connection with current instance fail or if provided alias doesn't exist.
     */
    public Date getExpiryTimeInDate(final String alias) throws QuasardbException {
        // Checks params
        this.checkSession();
        this.checkAlias(alias);

        // Init
        long time = 0;
        error_carrier error = new error_carrier();
        
        // Get value associated with alias
        time = qdb.get_expiry(session, alias, error);
        
        // Handle errors
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QuasardbException(error.getError());
        }

        return new Date(time * 1000);
    }
    
    /**
     * Change the expiry time in seconds for a provided alias
     * 
     * @since 1.1.0
     * 
     * @param alias the object's unique key/alias.
     * @param expiryTime the expiry time in second related to the provided alias (0 means eternal)
     * @throws QuasardbException if the connection with current instance fail or if provided alias doesn't exist.
     */
    public void setExpiryTimeInSeconds(final String alias, final long expiryTime) throws QuasardbException {
        // Checks params
        this.checkSession();
        this.checkAlias(alias);

        // Get value associated with alias
        qdb_error_t qdbError = null;
        if (expiryTime > 0L) {
            qdbError = qdb.expires_from_now(session, alias, expiryTime);
        } else {
            qdbError = qdb.expires_at(session, alias, 0);
        }
     
        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }
    }
    
    /**
     * Change the expiry time for a provided alias at the provided date
     * 
     * @since 1.1.0
     * 
     * @param alias the object's unique key/alias.
     * @param expiryDate the expiry date related to the provided alias.
     * @throws QuasardbException if the connection with current instance fail or if provided alias doesn't exist.
     */
    public void setExpiryTimeAt(final String alias, final Date expiryDate) throws QuasardbException {
        // Checks params
        this.checkSession();
        this.checkAlias(alias);
        if (expiryDate == null) {
            throw new QuasardbException(WRONG_DATE);
        }

        // Set expiry date with alias
        qdb_error_t qdbError = null;
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(expiryDate);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        qdbError = qdb.expires_at(session, alias, cal.getTimeInMillis()/1000);
     
        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }
    }
    
    /**
     * Get the default expiry time in seconds for all new entries. 
     * Return the 0 value if entries are eternal
     * 
     * @since 1.1.0
     * 
     * @return 0 if entries are eternal, a value in seconds instead.
     */
    public long getDefaultExpiryTimeInSeconds() {
        return defaultExpiryTime;
    }
    
    /**
     * Set the default expiry time in seconds for all next entries.
     * 
     * @since 1.1.0
     * 
     * @param expiryTime expiry time in seconds to set up
     */
    public void setDefaultExpiryTimeInSeconds(long expiryTime) {
        this.defaultExpiryTime = (expiryTime < 0 ? 0 : expiryTime);
    }
    
    /**
     * Retrieve the topology of the current quasardb instance.
     *
     * @since 0.7.4
     * 
     * @param node the host of the quasardb node you want to retrieve topology
     * @param port the port of the quasardb node you want to retrieve topology
     * @return topology of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public NodeTopology getNodeTopology(final String node, final int port) throws QuasardbException {
        try {
            return genson.deserialize(this.getNodeInfo(node, port, "topology"), NodeTopology.class);
        } catch (Exception e) {
            throw new QuasardbException(e);
        }
    }
    
    /**
     * Stop the current node with a given reason.
     * 
     * @since 0.7.4
     * 
     * @param reason the reason to stop the selected node.
     */
    public void stopCurrentNode(final String reason) throws QuasardbException {
        qdb_error_t qdbError = null;
        final qdb_remote_node_t remote_node = new qdb_remote_node_t();
        remote_node.setAddress(config.get("host"));
        remote_node.setPort(Integer.parseInt(config.get("port")));
        qdbError = qdb.stop_node(session, remote_node, reason);
        
        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }
    }
    
    /**
     * Stop a specific node with a given reason.
     * 
     * @since 0.7.4
     * 
     * @param node the host of the quasardb node you want to stop - can be a IP address or a hostname.
     * @param port the port of the quasardb node you want to stop.
     * @param reason the reason to stop the selected node.
     */
    public void stopNode(final String node, final int port, final String reason) throws QuasardbException {
        qdb_error_t qdbError = null;
        final qdb_remote_node_t remote_node = new qdb_remote_node_t();
        remote_node.setAddress(node);
        remote_node.setPort(port);
        qdbError = qdb.stop_node(session, remote_node, reason);
        
        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }
    }
    
    /**
     * Utility method to retrieve information operations on the current qdb instance.<br>
     *
     * @since 0.7.4
     * 
     * @param node the host of the quasardb node you want to stop - can be a IP address or a hostname.
     * @param port the port of the quasardb node you want to stop.
     * @param operation operation to apply on the current node.
     * @return the information related to the information operation.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    private String getNodeInfo(final String node, final int port, final String operation) throws QuasardbException {
        this.checkSession();
        String result = "";
        final qdb_remote_node_t remote_node = new qdb_remote_node_t();
        remote_node.setAddress(config.get("host"));
        remote_node.setPort(Integer.parseInt(config.get("port")));
        qdb.multi_connect(session, remote_node, 1);
        final error_carrier error = new error_carrier();
        ByteBuffer buffer;
        if (operation.equalsIgnoreCase("config")) {
            buffer = qdb.node_config(session, remote_node, error);
        } else if (operation.equalsIgnoreCase("topology")) {
            buffer = qdb.node_topology(session, remote_node, error);
        } else {
            buffer = qdb.node_status(session, remote_node, error);
        }
        try {
            if (buffer != null) { 
                result = decoder.decode(buffer).toString();
            } 
        } catch (CharacterCodingException e) {
            throw new QuasardbException(e.getMessage(), e);
        } finally {
            qdb.free_buffer(session, buffer);
            buffer = null;
        }
        return result;
    }
    
    /**
     * Get the entry associated with the supplied unique key (<i>alias</i>).
     * 
     * @since 0.5.2
     *  
     * @param alias the object's unique key/alias.
     * @return the object's related to the alias
     * @throws QuasardbException if an error occurs or the entry does not exist
     */
    @SuppressWarnings("unchecked")
    public <V> V get(final String alias) throws QuasardbException {
        // Checks params
        this.checkSession();
        this.checkAlias(alias);

        // Init
        V result = null;
        error_carrier error = new error_carrier();
        
        // Get value associated with alias
        ByteBuffer buffer = qdb.get_buffer(session, alias, error);
        
        // Prepare ByteBuffer
        if (buffer != null) {
            buffer.rewind();
        }
        
        // Handle errors
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QuasardbException(error.getError());
        }

        // De-serialize
        try {
            result = (V) serializer.readClassAndObject(new Input(new ByteBufferInputStream(buffer)));
        } catch (Exception e) {
            throw new QuasardbException(e.getMessage(), e);
        } finally {
            // Free ressources
            qdb.free_buffer(session, buffer);
            buffer = null;
            error = null;
        }

        return result;
    }
    
    /**
     * Atomically get the entry associated with the supplied unique key (<i>alias</i>) and remove it.
     *
     * @since 0.7.3
     *
     * @param alias the object's unique key/alias.
     * @return the object's related to the alias
     * @throws QuasardbException if an error occurs or the entry does not exist
     */
    @SuppressWarnings("unchecked")
    public <V> V getRemove(final String alias) throws QuasardbException {
        // Checks params
        this.checkSession();
        this.checkAlias(alias);

        // Init
        V result = null;
        error_carrier error = new error_carrier();
        
        // Get value associated with alias
        ByteBuffer buffer = qdb.get_remove(session, alias, error);
        
        // Prepare ByteBuffer
        if (buffer != null) {
            buffer.rewind();
        }
        
        // Handle errors
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QuasardbException(error.getError());
        }

        // De-serialize
        try {
            result = (V) serializer.readClassAndObject(new Input(new ByteBufferInputStream(buffer)));
        } catch (Exception e) {
            throw new QuasardbException(e.getMessage(), e);
        } finally {
            // Free ressources
            qdb.free_buffer(session, buffer);
            buffer = null;
            error = null;
        }
        
        return result;
    }

    /**
     * Adds an entry (<i>value</i>) to the current qdb instance under the <i>alias</i> key. The entry must not already exist.<br>
     *
     * @since 0.5.2
     *
     * @param alias a key to uniquely identify the entry within the cluster.
     * @param value object to associate to the key.
     * @throws QuasardbException if an error occurs or the entry already exists.
     */
    public <V> void put(final String alias, final V value) throws QuasardbException {
        this.writeOperation(alias, value, null, PUT);
    }
    
    /**
     * Adds an entry (<i>value</i>) to the current qdb instance under the <i>alias</i> key. The entry must not already exist.<br>
     *
     * @since 1.1.0
     * 
     * @param alias a key to uniquely identify the entry within the cluster.
     * @param value object to associate to the key.
     * @param expiryTime expiry time in seconds associate to the key. The provided value is prior to the default expiry time.
     * @throws QuasardbException if an error occurs or the entry already exists.
     */
    public <V> void put(final String alias, final V value, final long expiryTime) throws QuasardbException {
        long expiryTemp = (expiryTime < 0) ? this.defaultExpiryTime : expiryTime;
        if (expiryTime < 0) {
            this.defaultExpiryTime = 0;
        }
            
        this.put(alias, value);

        if (expiryTime < 0) {
            this.defaultExpiryTime = expiryTemp;
        } else {
            qdb_error_t qdbError = qdb.expires_from_now(session, alias, expiryTemp);
            if (qdbError != qdb_error_t.error_ok) {
                throw new QuasardbException(qdbError);
            }
        }
    }
    

    /**
     * Update an existing entry or create a new one.<br>
     *
     * @since 0.5.2
     *
     * @param alias a key to uniquely identify the entry within the cluster.
     * @param value the new object to associate to the key
     * @throws QuasardbException if an error occurs.
     */
    public <V> void update(final String alias, final V value) throws QuasardbException {
        this.writeOperation(alias, value, null, UPDATE);
    }

    /**
     * Update an existing alias with data and return its previous value
     *
     * @param alias a key to uniquely identify the entry within the cluster
     * @param value the new object to associate to the key
     * @return the previous value associated to the key
     * 
     * @since 0.7.3
     */
    public <V> V getAndReplace(final String alias, final V value) throws QuasardbException {
        return this.writeOperation(alias, value, null, GETANDUPDATE);
    }

    /**
     * Compare an existing alias with comparand, updates it to new if they match and return the original value
     *
     * @param alias a key to uniquely identify the entry within the cluster
     * @param value the new object to associate to the key
     * @param comparand the object to compare with original value associated to the key
     * @return the original value associated to the key
     * 
     * @since 0.7.3
     */
    public <V> V compareAndSwap(final String alias, final V value, final V comparand) throws QuasardbException {
        return this.writeOperation(alias, value, comparand, CAS);
    }

    /**
     * Delete the object associated with the <i>alias</i> key.
     *
     * @since 0.5.2
     *
     * @param alias the alias you want to delete.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public void remove(final String alias) throws QuasardbException {
        // Check params
        this.checkSession();
        this.checkAlias(alias);

        // Delete the entry on Quasardb instance
        final qdb_error_t qdbError = qdb.remove(session, alias);

        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }
    }

    /**
     * Delete all the stored objects in the current quasardb instance. Use with caution.
     *
     * @since 0.7.2
     *
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public void removeAll() throws QuasardbException {
        // Checks params
        this.checkSession();

         // Delete the entry on Quasardb instance
        final qdb_error_t qdbError = qdb.remove_all(session);

        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }
    }
    
    /**
     * Delete the object associated whith the <i>alias</i> key if the object is equal to comparand
     * 
     * @since 0.7.2
     * 
     * @param alias the alias you want to delete
     * @param comparand the object you want to compare with
     * @throws QuasardbException if the connecion with the current instance fail
     */
    public <V> void removeIf(final String alias, final V comparand) throws QuasardbException {
        // Check params
        this.checkSession();
        this.checkAlias(alias);
        
        if (comparand == null) {
            throw new QuasardbException(NULL_VALUE);
        }
        
        // Allocate buffer :
        //  -> intialize with default value
        int bufferSize = BUFFER_SIZE;

        //  -> try to evaluate the size of the object at runtime using sizeOf
        try {
            bufferSize = (int) RamUsageEstimator.sizeOf(comparand);
            if (bufferSize == 0) {
                bufferSize = BUFFER_SIZE;
            }
        } catch (Exception e) {
            throw new QuasardbException(BAD_SIZE, e);
        }

        // Get a direct byte buffer from pool with the specified size
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
        try {
            Output output = new Output(bufferSize);
            serializer.writeClassAndObject(output, comparand);
            buffer.put(output.getBuffer());

            // Apply remove if
            final qdb_error_t qdbError = qdb.remove_if(session, alias, buffer, buffer.limit());
    
            // Handle errors
            if (qdbError != qdb_error_t.error_ok) {
                throw new QuasardbException(qdbError);
            }
        } catch (Exception e) {
            throw new QuasardbException(BAD_SERIALIZATION, e);
        } finally {
            // Cleanup
            try {
                if (buffer != null) {
                    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                    if (cleaner != null) cleaner.clean();
                }
            } catch (Exception e) {
                throw new QuasardbException(SESSION_CLOSED, e);
            }
            buffer = null;
        }
    }
    
    /**
     * Submit a list of operation which can increase performance when it is necessary to run many small operations.
     * Using properly the batch operations requires initializing, running list of operations and read results
     * 
     * @param operations List of operations to submit in batch mode to Quasardb. See {@link Operation}
     * @return All results of submitted operations in batch mode.  See {@link Results}
     * 
     * @since 1.1.0
     * 
     */
    @SuppressWarnings("unchecked")
    public <V> Results runBatch(List<Operation<V>> operations) throws QuasardbException {
        // Check params
        this.checkSession();
        if (operations == null) {
            throw new QuasardbException(NULL_VALUE);
        }
        
        BatchOpsVec requests = new BatchOpsVec();
        Results results = null;
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
        
        // Prepare all batch requests
        for (Operation<V> operation : operations) {
            if (operation.getType() != null) {
                qdb_operation_t req = new qdb_operation_t();
                
                // Set operation type
                switch (operation.getType()) {
                    case GET :
                        req.setType(qdb_operation_type_t.optionp_get_alloc);
                        break;
                        
                    case PUT :
                        req.setType(qdb_operation_type_t.optionp_put);
                        break;
                        
                    case UPDATE :
                        req.setType(qdb_operation_type_t.optionp_update);
                        break;
                        
                    case REMOVE :
                        req.setType(qdb_operation_type_t.optionp_remove);
                        break;
                        
                    case CAS :
                        req.setType(qdb_operation_type_t.optionp_cas);
                        break;
                        
                    case GET_UPDATE :
                        req.setType(qdb_operation_type_t.optionp_get_update);
                        break;
                        
                    case GET_REMOVE :
                        req.setType(qdb_operation_type_t.optionp_get_remove);
                        break;
                        
                    case REMOVE_IF:
                        req.setType(qdb_operation_type_t.optionp_remove_if);
                        break;
                        
                    default :
                        req.setType(qdb_operation_type_t.optionp_uninitialized);
                        break;
                }
                
                // Set alias
                if ((operation.getAlias() != null) && (!operation.getAlias().equals(""))) {
                    req.setAlias(operation.getAlias());
                }
                
                // Prepare content
                if ((operation.getValue() != null) && (req.getType() != qdb_operation_type_t.optionp_remove_if)) {
                    ByteBuffer buffer = null;
                    try {
                        int bufferSize = (int) RamUsageEstimator.sizeOf(operation.getValue());
                        if (bufferSize == 0) {
                            bufferSize = BUFFER_SIZE;
                        }
                        buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
                        Output output = new Output(bufferSize);
                        serializer.writeClassAndObject(output, operation.getValue());
                        buffer.put(output.getBuffer());
                        req.setContent_size(bufferSize);
                        req.setContent(buffer);
                        
                        buffers.add(buffer);
                    } catch (Exception e) {
                        req.setContent_size(0);
                        req.setContent(null);
                        req.setType(qdb_operation_type_t.optionp_uninitialized);
                    } 
                }
                
                // Prepare comparand content
                if ((operation.getCompareValue() != null) || ((req.getType() == qdb_operation_type_t.optionp_remove_if) && (operation.getValue() != null))) {
                    ByteBuffer buffer = null;
                    V comparandValue = (req.getType() == qdb_operation_type_t.optionp_remove_if)?operation.getValue():operation.getCompareValue();
                    try {
                        int bufferSize = (int) RamUsageEstimator.sizeOf(comparandValue);
                        if (bufferSize == 0) {
                            bufferSize = BUFFER_SIZE;
                        }
                        buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
                        Output output = new Output(bufferSize);
                        serializer.writeClassAndObject(output, comparandValue);
                        buffer.put(output.getBuffer());
                        req.setComparand_size(bufferSize);
                        req.setComparand(buffer);
                        
                        buffers.add(buffer);
                    } catch (Exception e) {
                        req.setComparand_size(0);
                        req.setComparand(null);
                        req.setType(qdb_operation_type_t.optionp_uninitialized);
                    }
                }
                
                // Prepare requests
                requests.push_back(req);
            }
        }
        
        // Run batch
        run_batch_result qdbResults = qdb.run_batch(session, requests);
        
        // Build results
        results = new Results();
        results.setSuccess(qdbResults.getSuccesses() == operations.size());
        for (int i = 0; i < operations.size(); i++) {
            qdb_operation_t operation = qdbResults.getResults().get(i);
            Result<V> result = new Result<V>();
            
            // Set alias
            result.setAlias(operation.getAlias());
            
            // Set error
            result.setSuccess(operation.getError() == qdb_error_t.error_ok);
            if (!result.isSuccess()) {
                result.setError(operation.getError().toString());
            }
            
            // Set type
            boolean hasValue = false;
            if (operation.getType() == qdb_operation_type_t.optionp_get_alloc) { // GET operation
                result.setTypeOperation(TypeOperation.GET);
                hasValue = true && result.isSuccess();
            } else if (operation.getType() == qdb_operation_type_t.optionp_put) { // PUT operation
                result.setTypeOperation(TypeOperation.PUT);
            } else if (operation.getType() == qdb_operation_type_t.optionp_update) { // UPDATE operation
                result.setTypeOperation(TypeOperation.UPDATE);
            } else if (operation.getType() == qdb_operation_type_t.optionp_remove) { // REMOVE operation
                result.setTypeOperation(TypeOperation.REMOVE);
            } else if (operation.getType() == qdb_operation_type_t.optionp_cas) { // COMPARE AND SWAP operation
                result.setTypeOperation(TypeOperation.CAS);
                hasValue = true && result.isSuccess();
            } else if (operation.getType() == qdb_operation_type_t.optionp_get_update) { // GET AND UPDATE operation
                result.setTypeOperation(TypeOperation.GET_UPDATE);
                hasValue = true && result.isSuccess();
            } else if (operation.getType() == qdb_operation_type_t.optionp_get_remove) { // GET AND REMOVE operation
                result.setTypeOperation(TypeOperation.GET_REMOVE);
                hasValue = true && result.isSuccess();
            } else if (operation.getType() == qdb_operation_type_t.optionp_remove_if) { // REMOVE IF operation
                result.setTypeOperation(TypeOperation.REMOVE_IF);
            } else { // NO OPERATION
                result.setTypeOperation(TypeOperation.NO_OP);
            }
            
            // Set value
            if (hasValue) {
                ByteBuffer buffer = operation.getResult();
                if (buffer != null) {
                    buffer.rewind();
                }
                
                try {
                    result.setValue((V) serializer.readClassAndObject(new Input(new ByteBufferInputStream(buffer))));
                } catch (Exception e) {
                    result.setValue(null);
                    result.setError(qdb_error_t.error_unmatched_content.toString());
                    result.setSuccess(false);
                }
            }
            
            // Add result           
            results.getResults().add(result);
        }
        
        // Cleanup buffers
        qdb.release_batch_result(session, qdbResults);
        for (ByteBuffer buffer : buffers) {
            try {
                if (buffer != null) {
                    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                    if (cleaner != null) cleaner.clean();
                }
            } catch (Exception e) {
               e.printStackTrace();
            }
        }
        buffers = null;
 
        return results;
    }

    /**
     * Prefix based search on all entries. 
     * Search is on entries (i.e. aliases) but not on values.
     * 
     * @since 1.1.0
     * 
     * @param prefix 
     * @return all matching entries, but not the associated content.
     * @throws QuasardbException
     */
    public List<String> startsWith(final String prefix) throws QuasardbException {
        // Check params
        this.checkSession();
        this.checkAlias(prefix);
        
        // Search aliases
        error_carrier error = new error_carrier();
        StringVec aliases = qdb.prefix_get(session, prefix, error);
        
        // Handle errors
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QuasardbException(error.getError());
        }
        
        // Build result
        List<String> resultat = new ArrayList<String>(); 
        if (aliases != null && !aliases.empty()) {
            for (int i = 0; i < aliases.size(); i++) {
                resultat.add(aliases.get(i));
            }
        }
        
        return resultat;
    }
    
    /**
     * Close the connection to the quasardb instance and frees resources.
     * 
     * @since 0.5.2
     * 
     * @throws QuasardbException if the connection to the quasardb instance cannot be closed
     */
    public void close() throws QuasardbException {
        // Check params
        this.checkSession();

        // Try to close qdb session
        final qdb_error_t qdbError = qdb.close(session);

        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }

        // Cleanup
        this.config = null;
        this.session = null;
    }

    
    /*********************/
    /** private methods **/
    /*********************/
    /**
     * Check if the current qdb session is valid
     * 
     * @since 0.5.2
     * 
     * @throws QuasardbException if the connection to the qdb instance cannot be closed
     */
    private final void checkSession() throws QuasardbException {
        if (session == null) {
            throw new QuasardbException(SESSION_CLOSED);
        }
    }

    /**
     * Check if the provided alias is valid
     * 
     * @since 0.5.2
     * 
     * @param alias to store object
     * @throws QuasardbException if the connection to the quasardb instance cannot be closed
     */
    private final void checkAlias(final String alias) throws QuasardbException {
         // Testing parameters
        //  -> A null or empty key is forbidden
        if ((alias == null) || (alias.length() == 0)) {
            throw new QuasardbException(WRONG_ALIAS);
        }
    }

    /**
     * Utility method to apply write operations in the current qdb instance.<br>
     * The main goal is to serialize an object into a <a href="http://download.oracle.com/javase/1.4.2/docs/api/java/nio/ByteBuffer.html">ByteBuffer</a> and store it into a qdb instance.
     * 
     * @since 0.5.3
     * 
     * @param alias alias under the object <i>value</i> will be stored.
     * @param value object to serialize and store into the qdb instance.
     * @param other other object to compare with value
     * @param operation to apply on the two first parameters.
     * @return the buffer containing the serialized form of the <i>value</i> object.
     * @throws QuasardbException if parameters are not allowed or if the provided value cannot be serialized.
     */
    @SuppressWarnings("unchecked")
    private final <V> V writeOperation(final String alias, final V value, final V other, final int operation) throws QuasardbException {
        // Checks params
        this.checkSession();
        this.checkAlias(alias);

        // Init
        V result = null;
        
        // Testing parameters :
        //  -> A null value is forbidden
        if (value == null) {
            throw new QuasardbException(NULL_VALUE);
        }

        // Allocate buffer :
        //  -> intialize with default value
        int bufferSize = BUFFER_SIZE;

        //  -> try to evaluate the size of the object at runtime using sizeOf
        try {
            bufferSize = (int) RamUsageEstimator.sizeOf(value);
            if (bufferSize == 0) {
                bufferSize = BUFFER_SIZE;
            }
        } catch (Exception e) {
            throw new QuasardbException(BAD_SIZE, e);
        }

        // Get a direct byte buffer from pool with the specified size
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());

        try {
            Output output = new Output(bufferSize);
            serializer.writeClassAndObject(output, value);
            buffer.put(output.getBuffer());

            // Put or update value into QuasarDB
            qdb_error_t qdbError = null;
            ByteBuffer bufferResult = null;
            error_carrier error = new error_carrier();
            switch (operation) {
                case PUT :
                    qdbError = qdb.put(session, alias, buffer, buffer.limit(), defaultExpiryTime);
                    break;
                case UPDATE :
                    qdbError = qdb.update(session, alias, buffer, buffer.limit(), defaultExpiryTime);
                    break;
                case CAS :
                    if (other == null) {
                        throw new QuasardbException(NULL_VALUE);
                    } else {
                        int otherSize = BUFFER_SIZE;
                        try {
                            otherSize = (int) RamUsageEstimator.sizeOf(other);
                            if (otherSize == 0) {
                                otherSize = BUFFER_SIZE;
                            }
                        } catch (Exception e) {
                            throw new QuasardbException(BAD_SIZE, e);
                        }
                        ByteBuffer otherBuffer = ByteBuffer.allocateDirect(otherSize).order(ByteOrder.nativeOrder());
                        Output otherOutput = new Output(otherSize);
                        serializer.writeClassAndObject(otherOutput, other);
                        otherBuffer.put(otherOutput.getBuffer());
                        bufferResult = qdb.compare_and_swap(session, alias, buffer, buffer.limit(), otherBuffer, otherBuffer.limit(), defaultExpiryTime, error);
                        qdbError = error.getError();
                    }
                    break;
                case GETANDUPDATE :
                    bufferResult = qdb.get_buffer_update(session, alias, buffer, buffer.limit(), defaultExpiryTime, error);
                    qdbError = error.getError();
                    break;
                default :
                    break;
            }

            // Handle errors
            if (qdbError != qdb_error_t.error_ok) {
                throw new QuasardbException(qdbError);
            }

            // Handle eventually results
            if (bufferResult != null) {
                bufferResult.rewind();
                try {
                    result = (V) serializer.readClassAndObject(new Input(new ByteBufferInputStream(bufferResult)));
                } catch (Exception e) {
                    throw new QuasardbException(e.getMessage(), e);
                } finally {
                    qdb.free_buffer(session, bufferResult);
                    bufferResult = null;
                }
            }
            
            return result;
        } catch (Exception e) {
            throw new QuasardbException(BAD_SERIALIZATION, e);
        } finally {
            // Cleanup
            try {
                if (buffer != null) {
                    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                    if (cleaner != null) cleaner.clean();
                }
            } catch (Exception e) {
                throw new QuasardbException(SESSION_CLOSED, e);
            }
            buffer = null;
        }
    }
    

    /***********************/
    /** getter and setter **/
    /***********************/
    /**
     * Get the current instance configuration.
     *
     * @since 0.5.2
     *
     * @return current quasardb configuration
     * @throws QuasardbException if the connection to the quasardb instance cannot be closed
     */
    public final Map<String, String> getConfig() {
        return config;
    }

    /**
     * Updates the configuration properties
     *
     * @since 0.5.2
     *
     * @param config configuration properties
     * @throws QuasardbException if the connection to the quasardb instance cannot be closed
     */
    public void setConfig(final Map<String, String> config) {
        this.config = config;
    }
    
    
    /**************/
    /** override **/
    /**************/
    @Override
    public String toString() {
        try {
            return "Quasardb - Version : " + getVersion() + " - Build : " + getBuild();
        } catch (QuasardbException e) {
            return "Quasardb - " + e.getMessage();
        }
    }

    
    /****************/
    /** implements **/
    /****************/
    /** 
     * Quasardb implements {@link Iterable} for {@link QuasardbEntry}, providing support for simplified iteration.
     * However iteration should be used with caution. It is an O(n) operation.
     * 
     * @since 1.0.0
     * @see java.lang.Iterable#iterator()
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterator<QuasardbEntry<?>> iterator() {
        return new QuasardbIterator(session);
    }
    
    /*****************/
    /** inner class **/
    /*****************/
    private final class QuasardbIterator<V> implements Iterator<QuasardbEntry<V>> {
        private QuasardbEntry<V> nextEntry = null;
        private QuasardbEntry<V> lastEntry = null;
        private transient qdb_const_iterator_t iterator = null;
        private boolean iteratorStarted = false;
        private transient SWIGTYPE_p_qdb_session session;
        
        private QuasardbIterator(final SWIGTYPE_p_qdb_session session) {
            this.session = session;
            nextEntry = null;
            lastEntry = null;
            iterator = null;
            iteratorStarted = false;
        }

        private final void startIterator() throws QuasardbException {
            // Checks params
            checkSession();
                
            // Start iterator operation
            this.iterator = new qdb_const_iterator_t();
            final qdb_error_t qdbError = qdb.iterator_begin(session, this.iterator);
            
            // Handle errors
            if (qdbError != qdb_error_t.error_ok) {
                throw new QuasardbException(qdbError);
            }
            iteratorStarted = true;
        }
        
        private final void closeIterator() throws QuasardbException {
            // Checks params
            checkSession();
            
            // End iterator operation
            final qdb_error_t qdbError = qdb.iterator_close(this.iterator);
            
            // Handle errors
            if (qdbError != qdb_error_t.error_ok) {
                throw new QuasardbException(qdbError);
            }
            
            // Reset variables
            iteratorStarted = false;
            nextEntry = null;
            lastEntry = null;
            iterator = null;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private final void fetch() throws QuasardbException {
            // Checks params
            checkSession();
            
            // Get next qdb entry
            final qdb_error_t qdbError = qdb.iterator_next(iterator);
            
            // Handle errors
            if (qdbError == qdb_error_t.error_ok) {        
                // Get alias value
                ByteBuffer buffer = qdb.iterator_content(iterator);
                
                // Prepare ByteBuffer
                if (buffer != null) {
                    buffer.rewind();
                }
        
                // De-serialize
                V value = null;
                try {
                    value = (V) serializer.readClassAndObject(new Input(new ByteBufferInputStream(buffer)));
                } catch (Exception e) {
                    throw new QuasardbException(e.getMessage(), e);
                } finally {
                    // Free ressources
                    qdb.free_buffer(session, buffer);
                    buffer = null;
                }
                
                // Prepare entry
                nextEntry = new QuasardbEntry(iterator.getAlias(), value);
            } else if (qdbError == qdb_error_t.error_alias_not_found) {
                nextEntry = null;
                this.closeIterator();
            } else {
                throw new QuasardbException(qdbError);
            }
        }
        
        /**
         * {@inheritDoc}
         */
        public boolean hasNext() {
            try {
                if (!iteratorStarted) {
                    this.startIterator();
                }
                if (nextEntry == null) {
                    fetch();
                }
                return nextEntry != null;
            } catch (QuasardbException e) {
                return false;
            }
        }
    
        /**
         * {@inheritDoc}
         */
        public QuasardbEntry<V> next() {
            if (hasNext()) {
                // Remember the lastEntry
                lastEntry = nextEntry;
    
                // Reset nextEntry to force fetching the next available entry
                nextEntry = null;
                return lastEntry;
            } else {
                throw new NoSuchElementException();
            }
        }
    
        /**
         * {@inheritDoc}
         */
        public void remove() {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

}
