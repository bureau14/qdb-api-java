package com.b14.qdb;

import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;

/**
 * Quasardb exceptions.<br>
 * 
 * @author &copy; <a href="https://www.quasardb.net/">quasardb</a> - 2015
 * @version master
 * @since 2.0.0
 */
public class QdbException extends Exception {
    private static final long serialVersionUID = -6260791297528424386L;
    private static final String NO_ERR_CODE = "No error code available";
    private static final String NO_MSG = "No message available";
    
    // The exception message
    private transient String message;
    
    // The exception code
    private transient String code;
    
    public QdbException(final String message) {
    	super();
    	this.message = message;
    	this.code = NO_ERR_CODE;
    }
    
    /**
     * 
     * @param qdbError
     */
    public QdbException(final qdb_error_t qdbError) {
        super();
        this.message = (qdbError != null) ? qdb.make_error_string(qdbError) : NO_MSG;
        this.code = (qdbError != null) ? qdbError.toString() : NO_ERR_CODE;
    }
    
    /**
     * Build a QuasardbException using a thrown exception
     * 
     * @param cause the exception to handle
     */
    protected QdbException(final Throwable cause) {
        super(cause);
        this.message = cause.getMessage();
        this.code = cause.getClass().toString();
    }
    
    /**
     * Get error code of Exception
     * 
     * @return error code
     */
    public String getCode() {
        return this.code;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see Throwable
     */
    @Override
    public String getMessage() {
        return this.message;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return this.code + " : " + this.message;
    }   
    
    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((code == null) ? 0 : code.hashCode());
        result = prime * result + ((message == null) ? 0 : message.hashCode());
        return result;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        QdbException other = (QdbException) obj;
        if (code == null) {
            if (other.code != null) {
                return false;
            }
        } else if (!code.equals(other.code)) {
            return false;
        }
        if (message == null) {
            if (other.message != null) {
                return false;
            }
        } else if (!message.equals(other.message)) {
            return false;
        }
        return true;
    }
}
