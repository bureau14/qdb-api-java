package net.quasardb.qdb;

import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;

/**
 * Exception thrown when there is no exception class matching the returned error code.
 */
public class QdbMiscException extends QdbException {

    // The exception code
    private int code;

    public QdbMiscException(qdb_error_t errorCode) {
        super(qdb.make_error_string(errorCode));
        this.code = errorCode.swigValue();
    }

    /**
     * Get error code of Exception
     *
     * @return error code
     */
    public int getCode() {
        return this.code;
    }
}
