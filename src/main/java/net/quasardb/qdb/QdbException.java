package net.quasardb.qdb;

import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;

/**
 * Quasardb exceptions.<br>
 * 
 * @author &copy; <a href="https://www.quasardb.net/">quasardb</a> - 2015
 * @version 2.0.0
 * @since 2.0.0
 */
public class QdbException extends Exception {
    
    // The exception message
    private transient String message;
    
    // The exception code
    private transient int code;
        
    /**
     * 
     * @param qdbError
     */
    public QdbException(final qdb_error_t qdbError) {
        super();
        this.message = qdb.make_error_string(qdbError) ;
        this.code = qdbError.swigValue();
    }
    
    /**
     * Build a QuasardbException using a thrown exception
     * 
     * @param cause the exception to handle
     */
    protected QdbException(final Throwable cause) {
        super(cause);
        this.message = cause.getMessage();
        this.code = 0;
    }
    
    /**
     * Get error code of Exception
     * 
     * @return error code
     */
    public int getCode() {
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
        return this.getMessage();
    }   

}
