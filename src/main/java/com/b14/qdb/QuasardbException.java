package com.b14.qdb;

import com.b14.qdb.jni.qdb_error_t;

/**
 * This class handles all qdb exceptions.<br>
 * 
 * @author &copy; <a href="http://www.bureau14.fr/">bureau14</a> - 2011
 * @version qdb 0.7.0
 * @since qdb 0.5.2
 */
public class QuasardbException extends Exception {

    // id for serialization
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
     * Provide a way to custom Quasardb message exception
     * @param message the specific message related to the exception
     */
    protected QuasardbException(final String message) {
        super(message);
        this.message = message;
    }

    /**
     * Build a QuasardbException using an already throwed exception
     * @param cause the exception to handle
     */
    protected QuasardbException(final Throwable cause) {
        super(cause);
    }

    /**
     * Build a QuasardbException with an already throwed exception and a custom message
     * @param message the custom message of exception
     * @param cause the exception to handle
     */
    protected QuasardbException(final String message, final Throwable cause) {
        super(message, cause);
        this.message = message;
    }
    
    /**
     * Handle all JNI errors by providing custom QuasardbException
     * @param jniError the JNI error to handle
     * @param params <i>(optional)</i> additionnal params to custom exception
     */
    protected QuasardbException(final qdb_error_t jniError, final Object... params) {
        super();

        this.message = "Unknown error";

        if (jniError == qdb_error_t.error_alias_already_exists) {
            this.message = "The alias is already mapped.";
        }
        if (jniError == qdb_error_t.error_alias_not_found) {
            this.message = "The provided alias does not exist.";
        }
        if (jniError == qdb_error_t.error_alias_too_long) {
            this.message = "The provided alias is too long.";
        }
        if (jniError == qdb_error_t.error_buffer_too_small) {
            this.message = "The buffer is too small to handle value data. Size must be more than " + ((int[]) params[0])[0];
        }
        if (jniError == qdb_error_t.error_host_not_found) {
            this.message = "Host not found.";
        }
        if (jniError == qdb_error_t.error_internal) {
            this.message = "Quasardb internal error.";
        }
        if (jniError == qdb_error_t.error_invalid_command) {
            this.message = "Invalid command.";
        }
        if (jniError == qdb_error_t.error_invalid_input) {
            this.message = "Invalid input.";
        }
        if (jniError == qdb_error_t.error_invalid_protocol) {
            this.message = "Invalid protocol.";
        }
        if (jniError == qdb_error_t.error_no_memory) {
            this.message = "Out of memory.";
        }
        if (jniError == qdb_error_t.error_system) {
            this.message = "Quasardb system error.";
        }
        if (jniError == qdb_error_t.error_timeout) {
            this.message = "The operation timed out.";
        }
        if (jniError == qdb_error_t.error_connection_refused) {
            this.message = "Connection refused.";
        }
        if (jniError == qdb_error_t.error_connection_reset) {
            this.message = "Connection reset by peer.";
        }
        if (jniError == qdb_error_t.error_unexpected_reply) {
            this.message = "Unexpected reply.";
        }
        if (jniError == qdb_error_t.error_not_implemented) {
            this.message = "Not implemented.";
        }
        if (jniError == qdb_error_t.error_unstable_hive) {
            this.message = "Unstable hive.";
        }
        if (jniError == qdb_error_t.error_protocol_error) {
            this.message = "Protocol error.";
        }
        if (jniError == qdb_error_t.error_outdated_topology) {
            this.message = "Outdated topology.";
        }
        if (jniError == qdb_error_t.error_wrong_peer) {
            this.message = "Wrong peer";
        }
        if (jniError == qdb_error_t.error_invalid_version) {
            this.message = "Invalid version";
        }
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
