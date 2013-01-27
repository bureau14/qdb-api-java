package com.b14.qdb;

import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Currency;
import java.util.GregorianCalendar;
import java.util.Map;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.tools.LibraryHelper;
import com.b14.qdb.tools.profiler.ObjectProfiler;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.SerializationException;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.shaded.org.objenesis.strategy.StdInstantiatorStrategy;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.ClassSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.CurrencySerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.StringBufferSerializer;
import de.javakaffee.kryoserializers.StringBuilderSerializer;
import de.javakaffee.kryoserializers.cglib.CGLibProxySerializer;

/**
 * Quasardb main abstraction layer.<br>
 *
 * The following operations are offered:
 *
 * <ul>
 *     <li><u>get :</u> get an entry.</li>
 *     <li><u>put :</u> create an entry.</li>
 *     <li><u>update :</u> update the value of an existing entry.</li>
 *     <li><u>remove :</u> delete an entry.</li>
 *     <li><u>removeall :</u> delete all entries. Use with caution.</li>
 *     <li><u>close :</u> close the connection.</li>
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
 * @author &copy; <a href="http://www.bureau14.fr/">bureau14</a> - 2013
 * @version Quasar DB 0.7.3
 * @since Quasar DB 0.5.2
 */
@SuppressWarnings("restriction")
public final class Quasardb {
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
    private static final String BAD_SERIALIZATION = "Bad serialization.";
    private static final int BUFFER_SIZE = 4096;
    private static final int PUT = 1;
    private static final int UPDATE = 2;
    private static final int CAS = 3;
    private static final int GETANDUPDATE = 4;

    // Serializer initialisation
    private final Kryo serializer = new Kryo();

    // Keep qdb session reference
    private transient SWIGTYPE_p_qdb_session session;

    // Configuration of the qdb instance
    private transient Map<String,String> config = null;

    public Quasardb() {
    }

    /**
     * Create a quasardb instance with the provided configuration map.<br>
     * The configuration map must have the following parameters :
     * <ul>
     *     <li><i>name:</i> the unique name under which the quasardb instance will be referenced/registered.</li>
     *     <li><i>host:</i> the host of the quasardb cluster you want to connect to - can be a IP address or a hostname.</li>
     *     <li><i>port: </i> the port of the quasardb cluster you want to connect to.</li>
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

        // Initialize serializer
        serializer.setRegistrationRequired(false);
        serializer.setReferences(false);
        serializer.setInstantiatorStrategy(new StdInstantiatorStrategy());

        serializer.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
        serializer.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
        serializer.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
        serializer.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer(serializer));
        serializer.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer(serializer));
        serializer.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer(serializer));

        //  -> Handle empty arrays
        serializer.register(Arrays.asList("").getClass(), new ArraysAsListSerializer(serializer));

        //  -> Handle String tools
        serializer.register(StringBuffer.class, new StringBufferSerializer(serializer));
        serializer.register(StringBuilder.class, new StringBuilderSerializer(serializer));

        //  -> Handle specific classes
        serializer.register(Class.class, new ClassSerializer(serializer));
        serializer.register(Currency.class, new CurrencySerializer(serializer));
        serializer.register(GregorianCalendar.class, new GregorianCalendarSerializer());
        serializer.register(InvocationHandler.class, new JdkProxySerializer(serializer));
        serializer.register(CGLibProxySerializer.CGLibProxyMarker.class, new CGLibProxySerializer(serializer));

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
     * @return build version of the current quasardb instance.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public String getBuild() throws QuasardbException {
        this.checkSession();
        return qdb.build();
    }

    /**
     * Get the entry associated with the supplied unique key (<i>alias</i>).
     *
     * @param alias the object's unique key/alias.
     * @return the object's related to the alias
     * @throws QuasardbException if an error occurs or the entry does not exist
     */
    @SuppressWarnings("unchecked")
    public <V> V get(final String alias) throws QuasardbException {
        this.checkSession();
        this.checkAlias(alias);

        V result = null;
        error_carrier error = new error_carrier();
        
        ByteBuffer buffer = qdb.get_buffer(session, alias, error);
        if (buffer != null) {
            buffer.rewind();
        } else {
            throw new QuasardbException(error.getError());
        }

        try {
            result = (V) serializer.readClassAndObject(new Input(new ByteBufferInputStream(buffer)));
        } catch (SerializationException e) {
            throw new QuasardbException(e.getMessage(), e);
        } finally {
            qdb.free_buffer(session, buffer);
            buffer = null;
        }

        return result;
    }

    /**
     * Adds an entry (<i>value</i>) to the current qdb instance under the <i>alias</i> key. The entry must not already exist.<br>
     *
     * @param alias a key to uniquely identify the entry within the cluster.
     * @param value object to associate to the key.
     * @throws QuasardbException if an error occurs or the entry already exists.
     */
    public <V> void put(final String alias, final V value) throws QuasardbException {
        this.writeOperation(alias, value, null, PUT);
    }

    /**
     * Update an existing entry or create a new one.<br>
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
     * @since 0.7.3
     */
    public <V> V getAndReplace(final String alias, final V value) throws QuasardbException {
        return this.writeOperation(alias, value, null, GETANDUPDATE);
    }

    /**
     * Compares an existing alias with comparand, updates it to new if they match and return the original value
     *
     * @param alias a key to uniquely identify the entry within the cluster
     * @param value the new object to associate to the key
     * @param comparand the object to compare with original value associated to the key
     * @return the original value associated to the key
     * @since 0.7.3
     */
    public <V> V compareAndSwap(final String alias, final V value, final V comparand) throws QuasardbException {
        return this.writeOperation(alias, value, null, CAS);
    }

    /**
     * Delete the object associated with the <i>alias</i> key.
     *
     * @param alias the alias you want to delete.
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public void remove(final String alias) throws QuasardbException {
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
     * @throws QuasardbException if the connection with the current instance fail.
     */
    public void removeAll() throws QuasardbException {
        this.checkSession();

         // Delete the entry on Quasardb instance
        final qdb_error_t qdbError = qdb.remove_all(session);

        // Handle errors
        if (qdbError != qdb_error_t.error_ok) {
            throw new QuasardbException(qdbError);
        }
    }

    /**
     * Close the connection to the quasardb instance and frees resources.
     *
     * @throws QuasardbException if the connection to the quasardb instance cannot be closed
     */
    public void close() throws QuasardbException {
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
     * @throws QuasardbException if the connection to the qdb instance cannot be closed
     */
    private final void checkSession() throws QuasardbException {
        if (session == null) {
            throw new QuasardbException(SESSION_CLOSED);
        }
    }

    /**
     * Check if the provided alias is valid
     * @param alias to store object
     * @throws QuasardbException if the connection to the Quasar DB instance cannot be closed
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
     * @param alias alias under the object <i>value</i> will be stored.
     * @param value object to serialize and store into the qdb instance.
     * @param operation to apply on the two first parameters.
     * @return the buffer containing the serialized form of the <i>value</i> object.
     * @throws QuasardbException if parameters are not allowed or if the provided value cannot be serialized.
     */
    @SuppressWarnings("unchecked")
    private final <V> V writeOperation(final String alias, final V value, final V other, final int operation) throws QuasardbException {
        this.checkSession();
        this.checkAlias(alias);

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
            bufferSize = ObjectProfiler.sizeof(value);
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
                    qdbError = qdb.put(session, alias, buffer, buffer.limit());
                    break;
                case UPDATE :
                    qdbError = qdb.update(session, alias, buffer, buffer.limit());
                    break;
                case CAS :
                    if (other == null) {
                        throw new QuasardbException(NULL_VALUE);
                    }
                    if (!(value instanceof String) || !(other instanceof String)) {
                        throw new QuasardbException("Compare and Swap are only on String objects");
                    }
                    bufferResult = qdb.compare_and_swap(session, alias, (String) value, ((String) value).length(), (String) other, ((String) other).length(), error);
                    qdbError = error.getError();
                    break;
                case GETANDUPDATE :
                    bufferResult = qdb.get_buffer_update(session, alias, buffer, buffer.limit(), error);
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
                } catch (SerializationException e) {
                    throw new QuasardbException(e.getMessage(), e);
                } finally {
                    qdb.free_buffer(session, bufferResult);
                    bufferResult = null;
                }
            }
            
            return result;
        } catch (SerializationException e) {
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
     * @return current quasardb configuration
     * @throws QuasardbException if the connection to the quasardb instance cannot be closed
     */
    public final Map<String, String> getConfig() {
        return config;
    }

    /**
     * Updates the configuration properties
     *
     * @param configuration properties
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
}
