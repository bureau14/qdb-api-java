package com.b14.qdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;

/**
 * quasardb exceptions.<br>
 * 
 * @author &copy; <a href="http://www.bureau14.fr/">bureau14</a> - 2013
 * @version Quasar DB 0.7.3
 * @since Quasar DB 0.5.2
 */
public class QuasardbException extends Exception {

    // Id for serialization
    private static final long serialVersionUID = 2196126450147576611L;

    // The exception message
    private transient String message;

    /**
     * Default constructor
     */
    protected QuasardbException() {
        super();
    }

    /**
     * Custom Quasardb message exception
     * @param message the specific message related to the exception
     */
    protected QuasardbException(final String message) {
        super(message);
        this.message = message;
    }

    /**
     * Build a QuasardbException using a thrown exception
     * @param cause the exception to handle
     */
    protected QuasardbException(final Throwable cause) {
        super(cause);
    }

    /**
     * Build a QuasardbException from a thrown exception and a custom message
     * @param message the custom message of exception
     * @param cause the exception to handle
     */
    protected QuasardbException(final String message, final Throwable cause) {
        super(message, cause);
        this.message = message;
    }
    
    /**
     * Handle all JNI errors by providing a custom QuasardbException
     * @param jniError the JNI error to handle
     * @param params <i>(optional)</i> additionnal params to custom exception
     */
    protected QuasardbException(final qdb_error_t jniError) {
        super();
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder());
        qdb.error_jni(jniError, buffer);
        buffer.rewind();
        this.message = new String(buffer.array());
    }

    @Override
    public String toString() {
        return this.message;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
